import json
import os
import glob
import csv
from datetime import datetime
from ffdc_util.ffdc2csv.ffdc_new import Ffdc
from ffdc_util.ffdc2csv.checkpoints import FIELD_NAMES
import pandas as pd
import configparser
from utils.util import CONF_FILE, LOGGER
import shutil
import math

config = configparser.ConfigParser()
config.read(CONF_FILE)


def process(loc_pattern, perf_file):
    # from ffdc_boot_ad.ffdc_util.ffdc2csv.ffdc import TOTAL_BOOT_NUM
    TOTAL_BOOT_NUM = 0
    report_info = {}
    with open(perf_file, 'w+') as file:
        f_csv = csv.DictWriter(file, fieldnames = FIELD_NAMES)
        f_csv.writeheader()
        for tzzfile in glob.glob(loc_pattern):
            if os.system("lzop -t \'{}\'".format(tzzfile)):
                # continue
                #The check of the integrity of the compressed file is fail
                return None

            try:
                ffdc = Ffdc(tzzfile)
            except Exception as e:
                print("\033[1;35mSkipped!!!\033[0m",e)
                # continue
                #The decompression of the file is fail
                return None

            if not ffdc.get_has_uefi_log_flag():
                ffdc.delete_temp_dir()
                # continue
                #There is no UEFI log
                return None
            filename = os.path.splitext(os.path.basename(tzzfile))[0]

            (boot_num, cpu_num, disk_num, memory_size, pci_num, mtm, sn, boot_mode, firmware_list,
                     ffdc_boot_list) = ffdc.generate_csv_infos_from_uefi()

            if not ffdc_boot_list:
                ffdc.delete_temp_dir()
                return None
            else:
                csv_info = {}
                csv_info['Boot_Mode'] = boot_mode
                csv_info['CPU_Num'] = cpu_num
                csv_info['Disk_Num'] = disk_num
                csv_info['Memory_Size'] = memory_size
                csv_info['PCI_Num'] = pci_num
                csv_info['Mtm'] = mtm
                csv_info['SN'] = sn
                csv_info['Filename'] = filename
                csv_info['Firmware_Version'] = 'Unknown'
                for l in ffdc_boot_list:
                    csv_info['Module'] = l['Module']
                    csv_info['Time'] = l['Time']
                    csv_info['Boot_ID'] = l['Boot_ID']
                    csv_info['Sub_Boot_ID'] = l['Sub_Boot_ID']
                    csv_info['Boot_Type'] = l['Boot_Type']
                    csv_info['Intact'] = l['Intact']
                    if len(firmware_list) >= boot_num and boot_num:
                        csv_info['Firmware_Version'] = firmware_list[boot_num-1]
                    else:
                        csv_info['Firmware_Version'] = 'Unknown'
                    

                    f_csv.writerow(csv_info)

                report_info = {}
                report_info['serverType'] = mtm
                server_conf = {}
                server_conf['firmwareVersion'] = csv_info['Firmware_Version']
                server_conf['cpuNumber'] = cpu_num
                server_conf['memSizeMB'] = memory_size
                server_conf['diskNumber'] = disk_num
                server_conf['pciNumber'] = pci_num
                report_info['serverConf'] = server_conf

                ffdc.delete_temp_dir()

    return report_info


def filter_csv_test(uefi_log, perf_file):
    # result_dir = os.path.dirname(perf_file)
    # filename = os.path.join(result_dir, 'all.log_pre.csv')
    if not os.path.exists(uefi_log):
        return None
    file_list = [uefi_log]
    return generate_perf(file_list, perf_file)


def filter_csv(location, perf_file):
    # from ffdc_boot_ad.common.ffdc2flow import Ffdc2flow
    from ffdc_util.ffdc2csv.preprocess import Preprocess
    file_list = []
    for tzzfile in glob.glob(location):
        preprocess = Preprocess(tzzfile)
        file_prefix, _ = os.path.splitext(os.path.basename(tzzfile))
        tmp_path = preprocess.ffdc.uncompress_temp_dir
        result_dir = os.path.dirname(perf_file)
        filename = os.path.join(result_dir, file_prefix + 'all.log_pre.csv')
        shutil.copyfile(preprocess.uefi_csv, filename)
        file_list.append(filename)
        shutil.rmtree(tmp_path)
    return generate_perf(file_list, perf_file)

def generate_perf(file_list, perf_file):
    # save to filter file
    file = open(perf_file, "w")
    order = FIELD_NAMES
    order.append('UUid')
    order.append('Start_time')
    order.append('Phase')
    total_boot_num = 0
    for i, filename in enumerate(file_list):
        process_data = pd.read_csv(filename, sep=',', low_memory=False, encoding='ISO-8859-1')
        process_data = process_data[(process_data['Is_module'] == 'Y')]
        process_data.insert(0, 'Module', process_data['Log'].apply(lambda x: x.split('(')[0]))
        process_data.insert(1, 'Time', process_data['Log'].apply(lambda x: int(x.split('~')[1].split('ms')[0])))
        process_data = process_data.drop('Log',axis=1)
        process_data = process_data.drop('Is_module',axis=1)
        process_data = process_data.reset_index(drop=True)
        process_data = process_data[order]
        
        if i == 0:
            process_data.to_csv(file, mode='a', index=False)
            total_boot_num += len(process_data.groupby(['Boot_ID']).size())
        else:
            process_data["Boot_ID"] = process_data["Boot_ID"].apply(lambda x: x + total_boot_num)
            process_data.to_csv(file, mode='a', index=False, header=False)
            total_boot_num += len(process_data.groupby(['Boot_ID']).size())
    file.close()

    report_info = {}
    report_info['serverType'] = process_data['Mtm'].head(1).values[0] if process_data['Mtm'].count() > 0 else ''
    server_conf = {}
    server_conf['firmwareVersion'] = process_data['Firmware_Version'].head(1).values[0]
    server_conf['cpuNumber'] = process_data['CPU_Num'].head(1).values[0]
    server_conf['memSizeMB'] = process_data['Memory_Size'].head(1).values[0]
    server_conf['diskNumber'] = process_data['Disk_Num'].head(1).values[0]
    server_conf['pciNumber'] = process_data['PCI_Num'].head(1).values[0]
    report_info['serverConf'] = server_conf
    return report_info


def generate_filter(filename, filter_file, data_type):
    process_data = pd.read_csv(filename, sep=',', low_memory=False)
    process_data.dropna(axis=0, how='any', subset=['Filename', 'Boot_ID', 'Sub_Boot_ID'], inplace=True)
    process_data['Mtm'].fillna('missing', inplace=True)
    process_data['SN'].fillna('missing', inplace=True)
    process_data = process_data.drop('UUid', 1)
    process_data = process_data.drop('Start_time', 1)
    if data_type == 'train':
        # perf train
        process_data = process_data[(process_data['Intact'] == 'Y')]
        rt_max = process_data.groupby(['Boot_ID'])['Sub_Boot_ID'].max() < 3
        b = rt_max[rt_max == True].index.tolist()
        df = pd.DataFrame(b, columns=['Boot_ID'])
        process_data = process_data[process_data['Boot_ID'].isin(df['Boot_ID'])]
        process_data = process_data.reset_index(drop=True)
    else:
        # perf test
        b_list = process_data.drop_duplicates(subset='Boot_ID', keep='first', inplace=False)
        if len(b_list) > 3:
            bt_list = b_list['Boot_ID'].tail(3).to_frame()
            process_data = process_data[process_data['Boot_ID'].isin(bt_list['Boot_ID'])]
        if len(b_list) > 1:
            process_data = process_data.reset_index(drop=True)

        # only save the first n subboot in one boot to speedy up the execution time
        latest_sub_boot_num = config.getint("detection", "latest_sub_boot_num")
        gp = process_data.groupby(['Boot_ID'])
        for bn, gd in gp:
            last_sub_boot_index = gd['Sub_Boot_ID'].max()
            if last_sub_boot_index > latest_sub_boot_num:
                process_data.drop(gd[gd['Sub_Boot_ID'] <= (last_sub_boot_index - latest_sub_boot_num)].index, inplace=True)
        ###
        if process_data.empty:
            print("empty data after filter!SystemExit!")
            return

    # save to filter file
    file = open(filter_file, "w")
    process_data.to_csv(file, mode='a', index=False)
    file.close()


def generate_calltime(input_file, calltime_file):
    # read file
    data = pd.read_csv(input_file, sep=',',low_memory=False)  # , chunksize=1000
    # time sort file
    output_file = open(calltime_file, "w")
    inputfile = open(input_file)
    header = inputfile.readline()
    output_file.write(header)
    inputfile.close()
    # group module ,time sort
    # data.sort_values(['Boot_ID', 'Sub_Boot_ID', 'Time'], inplace=True,
    #                          ascending=[1, 1, 1])  # fixme wanghx order
    gp = data.groupby(['Boot_ID', 'Sub_Boot_ID'])
    for (b,sb),group in gp:
        data_cal = group.copy()
        data_cal['Time'] = data_cal['Time'].shift(-1) - data_cal['Time']
        # del NaN row
        data_cal.dropna(axis=0, how='any', inplace=True)
        data_cal.to_csv(output_file, mode='a', index=False, header=0)
    output_file.close()


def generate_boot(filename, boot_file, data_type):
    process_data = pd.read_csv(filename, sep=',', low_memory=False)
    # boot file
    TEST_FIELD_NAMES = ['Boot_ID', 'Sub_Boot_ID', 'Boot_Mode', 'Boot_Type', 'Intact', 'CPU_Num', 'Disk_Num',
                        'Memory_Size', 'PCI_Num', 'Mtm', 'Firmware_Version', 'SN', 'Filename',
                        'Boot_Times',  'Boot_Time', 'Restart_Times', 'Sub_Boot_Time',
                        'PEI_Time', 'PEI_Modules_Num', 'DXE_Time', 'DXE_Modules_Num', 'BDS_Time', 'BDS_Modules_Num']
                        # 'Boot_Start', 'Sub_Boot_Start'
                        #todo if add them, knnProcess 304 nbrs.fit(X_ohe) will raise Exception, could not convert string to float: '2019-10-15 06:27:16'
    sub_boot_row_data = pd.DataFrame(columns=TEST_FIELD_NAMES)

    boot_times = process_data['Boot_ID'].max()
    gp = process_data.groupby(['Boot_ID'], sort=False)
    for i, boot in gp:
        # boot_start = boot['Start_time'].head(1).values[0]
        boot_time = 0
        restart_times = boot['Sub_Boot_ID'].max()
        sgp = boot.groupby(['Sub_Boot_ID'])
        for si, sboot in sgp:
            pei_t, dxe_t, bds_t = (0, 0, 0)
            sub_boot_time = int(sboot['Time'].tail(1).values[0] - sboot['Time'].head(1).values[0])
            # sub_boot_start = sboot['Start_time'].head(1).values[0]
            boot_time += sub_boot_time
            pei_phase = sboot[sboot['Phase'] == 'PEI']
            # pei_t = int(pei_phase['Time'].max()) - int(pei_phase['Time'].min())
            pei_n = len(pei_phase)

            dxe_phase = sboot[sboot['Phase'] == 'DXE']
            # dxe_t = dxe_phase['Time'].max() - dxe_phase['Time'].min()
            dxe_n = len(dxe_phase)

            bds_phase = sboot[sboot['Phase'] == 'BDS']
            # bds_t = bds_phase['Time'].max() - bds_phase['Time'].min()
            bds_n = len(bds_phase)

            if pei_n != 0:
                if dxe_n != 0:
                    pei_t = dxe_phase['Time'].head(1).values[0] - pei_phase['Time'].head(1).values[0]
                else:
                    pei_t = pei_phase['Time'].tail(1).values[0] - pei_phase['Time'].head(1).values[0]
            if dxe_n != 0:
                if bds_n != 0:
                    dxe_t = bds_phase['Time'].head(1).values[0] - dxe_phase['Time'].head(1).values[0]
                else:
                    dxe_t = dxe_phase['Time'].tail(1).values[0] - dxe_phase['Time'].head(1).values[0]
            if bds_n != 0:
                bds_t = bds_phase['Time'].tail(1).values[0] - bds_phase['Time'].head(1).values[0]

            add_attr = ['Boot_Times', 'Boot_Time', 'Restart_Times', 'Sub_Boot_Time',
                        'PEI_Time', 'PEI_Modules_Num', 'DXE_Time', 'DXE_Modules_Num', 'BDS_Time', 'BDS_Modules_Num']
                        # 'Boot_Start', 'Sub_Boot_Start',

            add_data = [boot_times, boot_time, restart_times, sub_boot_time, pei_t, pei_n,
                        dxe_t, dxe_n, bds_t, bds_n] #boot_start, sub_boot_start
            add_subboot_data = pd.DataFrame([add_data], columns=add_attr)

            sub_attr = ['Boot_ID', 'Sub_Boot_ID', 'Boot_Mode', 'Boot_Type', 'Intact', 'CPU_Num', 'Disk_Num',
                        'Memory_Size', 'PCI_Num', 'Mtm', 'Firmware_Version', 'SN', 'Filename']

            sub_boot_data = sboot[sub_attr].head(1)
            sub_boot_data = sub_boot_data.reset_index(drop=True)  # reset index
            result = sub_boot_data.join(add_subboot_data, how='outer')
            sub_boot_row_data = sub_boot_row_data.append(result, sort=False)


    file = open(boot_file, "w")
    if data_type == 'test':
        sub_boot_row_data.sort_values(by=['Boot_ID'], ascending=False, inplace=True)
    sub_boot_row_data = sub_boot_row_data.reset_index(drop=True)  # reset index

    x = sub_boot_row_data[['Boot_Type', 'Mtm']]

    # one-hot Encoder
    X_ohe = pd.get_dummies(x)
    result = sub_boot_row_data.join(X_ohe)
    result.to_csv(file, mode='a', index=False)
    file.close()

    print("boot2csv finish!")


def extract_boot_info(filename, result_dir):
    process_data = pd.read_csv(filename, sep=',', low_memory=False, encoding='ISO-8859-1')
    file_gp = process_data.groupby('Filename')
    boot_list = []
    for name, file_data in file_gp:
        boot_gp = file_data.groupby('Boot_ID')
        device_id = file_data['UUid'].head(1).values[0]

        first_start_time = file_data['Start_time'].head(3).tail(1).values[0]
        if isinstance(first_start_time, float) and math.isnan(first_start_time):
            if file_data['Boot_ID'].max() > 2:
                first_start_time = process_data[(process_data['Boot_ID'] == 2)]['Start_time'].head(3).tail(1).values[0]
            else:
                first_start_time = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')

        for i, boot in boot_gp:
            start_time = boot['Start_time'].head(3).tail(1).values[0]
            if isinstance(start_time, float) and math.isnan(start_time):
                start_time = first_start_time
            boot_time = 0
            sboot_gp = boot.groupby(['Sub_Boot_ID'])
            for si, sboot in sboot_gp:
                sub_boot_time = int(sboot['Time'].tail(1).values[0] - sboot['Time'].head(1).values[0])
                boot_time += sub_boot_time
            boot_uuid = str(start_time) + '_' + str(boot['Time'].head(1).values[0])
            boot_dic = {
                # "boot_UUID": boot_uuid,
                # "deviceUUID": device_id,
                "boot_time_ms": boot_time,
                # "startTimestamp": datetime.strptime(start_time, '%Y-%m-%d %H:%M:%S')
                "boot_start_time": start_time
            }
            boot_list.append(boot_dic)

    boot_time_file = os.path.splitext(os.path.basename(filename))[0].split("_perf")[0] + ".json"
    boot_time_file = os.path.join(result_dir, boot_time_file)
    boot_time_fh = open(boot_time_file, "w")
    boot_time_fh.writelines(json.dumps(boot_list))
    boot_time_fh.close()


def write_boot_time(uefi_log, boot_perf_file, result_dir):
    # perf_file = os.path.join(result_dir, "perf.csv")
    report_info = filter_csv_test(uefi_log, boot_perf_file)
    if report_info is None:
        LOGGER.error('did not get boottime from perfcsv!')
        return None, None  # the input of the compressed file is not valid
    else:
        report_internal_json = report_info
    json_result = {}
    report_internal_json["result"] = json_result

    extract_boot_info(boot_perf_file, result_dir)
