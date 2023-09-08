import copy
from datetime import datetime, timedelta
import io
import os
import re
import tarfile
import tempfile
import time
import shutil
from ffdc_util.ffdc2csv.checkpoints import CP_LIST


def file_accessible(file_path, mode):
    """
    check if a file exists and is accessible"
    """
    try:
        f = open(file_path, mode)
        f.close()
    except IOError as e:
        return False

    return True


def extract_file(location, target_path):
    if (location.endswith("tar.gz") or location.endswith(".tar")
            or location.endswith('.tgz')):
        tar = tarfile.open(location)
        try:
            tar.extractall(target_path)
        except tarfile.ReadError as e:
            print('\033[1;35mTruncated log file\033[0m: {}'.format(location))
        tar.close()
    elif (location.endswith(".zip")):
        pass
    else:
        pass


TOTAL_BOOT_NUM = 0


class Ffdc(object):
    """
    FFDC file class
    """
    uefilog_dir = "var\\log\\hlog"
    uefilog_prefix = "uefilog_"
    service_file = "tmp\\service.txt"
    sensor_file = "tmp\\ffdc_live_dbg\\sensor.log"
    # fru_file = "tmp/ffdc_live_dbg/vertiv_live_dbg/vertiv_ipmitool_fru.log"
    cim_memory_smbios_file = "tmp\\ffdc_live_dbg\\vertiv_live_dbg\\cim_memory_smbios.txt"
    dmdb_pci_dumper_file = "tmp\\ffdc_live_dbg\\dmdb_pci_dumper.txt"
    cem_eventlog = "var\\volatile\\log\\cem_eventlog"

    def __init__(self, filepath):
        
        self.filepath = filepath
        directory = os.path.dirname(filepath)
        filename = os.path.basename(filepath)
        new_filename = os.path.splitext(filename)[0]+'.log'
        new_filepath = os.path.join(directory,new_filename)
        self.has_uefi_log = True
        (_, self.uncompress_temp_dir) = self._uncompress_tzz_file()
        self.merged_file = os.path.join(self.uncompress_temp_dir, "all.log")
        self._merge_uefi_log(self.merged_file)
        shutil.copy(self.merged_file, new_filepath)   
        self.first_shift_time = None

    def get_has_uefi_log_flag(self):
        return self.has_uefi_log

    def get_uncompress_temp_dir(self):
        return self.uncompress_temp_dir

    def delete_temp_dir(self):
        shutil.rmtree(self.uncompress_temp_dir)

    def _uncompress_tzz_file(self):
        """
        uncompress tzz file into temporary dir for later processing
        """
        filename = os.path.splitext(os.path.basename(self.filepath))[0]
        tempdir = tempfile.mkdtemp(suffix="_" + filename)
        cmd = "lzop -d -c \"{}\" | tar xf - -C {} ".format(self.filepath, tempdir)

        try:
            rtn = os.system(cmd)
        except Exception as e:
            print(e)
        print(self.filepath, "uncompress dir is:", tempdir)
        return rtn, tempdir

    # merge by index
    def _merge_uefi_log(self, merged_file):
        """
        merge all uefilog_*.tgz into one whole uefi logget_mtm_fv_sn
        """
        uefi_dst = os.path.join(self.uncompress_temp_dir, Ffdc.uefilog_dir)
        list = os.listdir(uefi_dst)
        if not list:
            self.has_uefi_log = False
            return
        index_name = []
        index_list = []
        first_uefi_log_index = 0

        for file in list:
            if "uefilog_" not in file or not file.endswith(".tgz"):
                continue
            i = re.split(r"[_,.]", file)
            index_name.append(int(i[1]))
            # untar uefilog_*.tgz
            file_name = os.path.join(uefi_dst, file)
            extract_file(file_name, uefi_dst)
        index_name.sort()
        print(index_name)
        list_100 = [x for x in range(1, 101)]
        if 1 in index_name and 100 in index_name:
            # uefi log have uefilog_1 and uefilog_100
            # 1-M N-100
            for k in reversed(list_100):
                # print(k)
                if k not in index_name:
                    first_uefi_log_index = k + 1
                    print("the first uefi log index is : {}".format(first_uefi_log_index))
                    break
            for k in list_100:
                if k not in index_name:
                    end_uefi_log_index = k
                    break
            a = [x for x in range(first_uefi_log_index, 101)]
            b = [x for x in range(1, end_uefi_log_index)]
            index_list = a + b
        else:
            first_uefi_log_index = index_name[0]
            print("the first uefi log index is : {}".format(first_uefi_log_index))
            index_list = index_name
        print(index_list)
       
        output_data = ""
        for index in index_list:
            file_name = "%s%s" % (Ffdc.uefilog_prefix, index)
            file_path = os.path.join(uefi_dst, file_name)
            if not file_accessible(file_path, 'r+'):
                continue

            with io.open(file_path, 'r+', encoding='ISO-8859-1') as file:
                output_data += file.read()

            file.close()

        with io.open(merged_file, 'w+', encoding='ISO-8859-1') as file:
            file.write(output_data)
            file.close()
        print("merged file: {}".format(merged_file))
        return merged_file

    # Desperated, please refer to _merge_uefi_log method.
    # merge by timestamp
    def _merge_uefi_log_v1(self, merged_file):
        """
        merge all uefilog_*.tgz into one whole uefi log
        """
        uefi_dst = os.path.join(self.uncompress_temp_dir, Ffdc.uefilog_dir)
        list = os.listdir(uefi_dst)
        if not list:
            self.has_uefi_log = False
            return
        index = 0

        for file in list:
            if "uefilog_" not in file or not file.endswith(".tgz"):
                continue
            file_path = os.path.join(uefi_dst, file)
            file_date = datetime.fromtimestamp(os.path.getmtime(file_path))
            if not index:
                first_uefi_file = file
                first_uefi_date = file_date
            else:
                if file_date < first_uefi_date:
                    first_uefi_file = file
                    first_uefi_date = file_date
            index += 1

            # untar uefilog_*.tgz
            file_name = os.path.join(uefi_dst, file)
            extract_file(file_name, uefi_dst)
        first_uefi_log_index = self.get_first_uefi_log_index(first_uefi_file)
        print("the first uefi log index is : {}".format(first_uefi_log_index))

        output_data = ""
        for index in range(first_uefi_log_index, 100) + range(
                1, first_uefi_log_index):
            file_name = "%s%s" % (Ffdc.uefilog_prefix, index)
            file_path = os.path.join(uefi_dst, file_name)
            if not file_accessible(file_path, 'r+'):
                continue

            with open(file_path, 'r+') as file:
                output_data += file.read()

            file.close()

        with open(merged_file, 'w+') as file:
            file.write(output_data)

        print("merged file: {}".format(merged_file))
        return merged_file

    def get_first_uefi_log_index(self, first_uefi_log):
        """
        get the 1st uefilog_*.tgz's index
        """
        hyphen_idx = first_uefi_log.index('_')
        dot_idx = first_uefi_log.index('.')
        return int(first_uefi_log[hyphen_idx+1:dot_idx])

    def get_mtm_fv_sn(self):
        """
        get machine type, firmware version, serial number from FFDC file
        """
        location = os.path.join(self.uncompress_temp_dir, Ffdc.service_file)
        (mtm, sn, uuid, uefi_version, uefi_build_id) = ["","","","",""]
        if not file_accessible(location,'r+'):
            print("{} not exist or can't be accessed".format(location))
            return (mtm, sn, uefi_version, uefi_build_id, uuid)
        found = False
        found_num = 0
        sn_flag = False
        with open(location, 'rt') as file:
            # for line in file:
            for num, line in enumerate(file):
                line = line.rstrip()
                if 'Type and Model' and 'Serial No' and 'UUID' in line:
                    found = True
                    found_num = num
                if re.match(r'^   (\w+)\s+(\w+)\s+(\w+)$',line):
                    mtm = re.match(r'^   (\w+)\s+(\w+)\s+(\w+)$',line).group(1)
                    # sometimes the mtm is 7X07, sometimes is 7X07RCZ000
                    # here we just use the first 4 digits
                    mtm = mtm[0:4]
                    sn = re.match(r'^   (\w+)\s+(\w+)\s+(\w+)$',line).group(2)
                    uuid = re.match(r'^   (\w+)\s+(\w+)\s+(\w+)$',line).group(3)
                    sn_flag = True
                if found == True and num == found_num + 2 and sn_flag == False:
                    found = False
                    MTM_PATTERN = re.compile(r'^   (\w+)\s+(.*)\s+(\w+)$')
                    m = MTM_PATTERN.match(line)
                    if m:
                        mtm = m.group(1)
                        # sometimes the mtm is 7X07, sometimes is 7X07RCZ000
                        # here we just use the first 4 digits
                        mtm = mtm[0:4]
                        sn = m.group(2)
                        uuid = m.group(3)

                
                if re.match(r'^   UEFI\s', line):
                    (uefi_version, uefi_build_id ) = line.split()[1:3]
            

        return (mtm, sn, uefi_version, uefi_build_id, uuid)

    def get_cpu_disk_info(self):
        """
        get cpu, memory, disk info from sensor file
        """
        location = os.path.join(self.uncompress_temp_dir, Ffdc.sensor_file)
        (cpu_num, disk_num) = (0, 0)

        if not file_accessible(location,'r+'):
            print("{} not exist or can't be accessed".format(location))
            return (cpu_num, disk_num)

        with open(location, 'rt') as file:
            for line in file:
                line = line.rstrip()
                if re.match(r'^CPU\d+\s+Temp|^CPU \d+\s+Temp',line):
                    cpu_num += 1
                elif re.match(r'^Drive\s\d+\s+\|\s0x0',line):
                    disk_num += 1

        return (cpu_num, disk_num)

    def get_mem_size(self):
        """
        get memory size from tmp/ffdc_live_dbg/vertiv_live_dbg/cim_memory_smbios.txt
        """
        location = os.path.join(self.uncompress_temp_dir, Ffdc.cim_memory_smbios_file)
        mem_size = 0
        if not file_accessible(location,'r+'):
            print("{} not exist or can't be accessed".format(location))
            return mem_size

        with open(location, 'rt') as file:
            for line in file:
                line = line.rstrip()
                if re.match(r'^Offset 0Ch',line):
                    mem_size += int(re.match(r'^Offset 0Ch.*\[(\d+)\]',line).group(1))

        return mem_size

    def get_pci_num(self):
        """
        get pci num from tmp/ffdc_live_dbg\dmdb_pci_dumper.txt
        """
        location = os.path.join(self.uncompress_temp_dir, Ffdc.dmdb_pci_dumper_file)
        pci_num = 0
        if not file_accessible(location,'r+'):
            print("{} not exist or can't be accessed".format(location))
            return pci_num

        with open(location, 'rt') as file:
            for line in file:
                line = line.rstrip()
                if re.match(r'^Location 1',line):
                    pci_num += 1

        return pci_num

    def get_cem_events(self):
        """
        get events list from cem_eventlog file
        :return: a list
        """
        location = os.path.join(self.uncompress_temp_dir, Ffdc.cem_eventlog)
        cem_events = []
        if not file_accessible(location, 'r+'):
            print("{} not exist or can't be accessed".format(location))
            return cem_events

        with open(location, 'r') as f_hd:
            for line in f_hd:
                one_event = dict()
                line = line.rstrip()
                fields = line.split(';')
                dt_hex = fields[1].split(':')   # 5BE1F47C:01C8
                ts = int(dt_hex[0], 16)
                ts_ms = int(dt_hex[1], 16)
                # time_array = time.localtime(ts)
                time_array = time.gmtime(ts)
                dt_str = time.strftime("%Y-%m-%d %H:%M:%S", time_array)
                dt_str += '.' + ts_ms.__str__()
                one_event['datetime'] = dt_str
                event_id_1 = fields[26]     # FQXSFMA0004N
                event_id_2 = fields[36]     # FQXSPMA0013N
                one_event['eventID'] = event_id_1
                cem_events.append(one_event)
        return cem_events

    def get_uefi_all_module_name(self):
        """
        get all module name from uefi merged log
        """
        s = set()
        with io.open(self.merged_file, 'r', encoding='ISO-8859-1') as file:
            for line in file:
                if re.match(r'(^PEIM\.\w+\.Entry).*~(\d+)ms', line):
                    module_name = re.match(r'(^PEIM\.\w+\.Entry).*~(\d+)ms', line).group(1)
                elif re.match(r'(^\w+\.Entry).*~(\d+)ms', line):  
                    module_name = re.match(r'(^\w+\.Entry).*~(\d+)ms', line).group(1)
                elif re.match(r'(^SMM\.\w+\.Entry).*~(\d+)ms', line):
                    module_name = re.match(r'(^SMM\.\w+\.Entry).*~(\d+)ms', line).group(1)
                elif re.match(r'(^\[.*\].Entry).*~(\d+)ms', line):
                    module_name = re.match(r'(^\[.*\]\.Entry).*~(\d+)ms', line).group(1)
                elif re.match(r'(^BDS\.\w+\.Entry).*~(\d+)ms', line):
                    module_name = re.match(r'(^BDS\.\w+\.Entry).*~(\d+)ms', line).group(1)
            
                else:
                    continue
                module_name = '\'' + module_name + '\''

                s.add(module_name)

        return s

    # Desperated, please refer to generate_csv_infos_from_uefi method.
    def get_infos_from_uefi(self):
        """
        get module delta time, firmware version, boot type, total boot num, 
        boot seq num from uefi
        """
        total_boot_num = 0
        boot_seq_num = 0
        warm_reset_num = 0
        boot_start, need_check_end_pattern = (False, False)
        end_pattern = re.compile(r'^\.$')
        one_boot_info = {}
        firmware_version = ""
        for cp in CP_LIST:
            one_boot_info[cp] = ""

        ffdc_boot_list = []
        boot_type = "DC"
        
        with io.open(self.merged_file, 'r', encoding='ISO-8859-1') as file:
            for line in file:
                line = line.rstrip()
                if boot_start:
                    # CmdCompleteEvt in before line and the boot already start
                    if need_check_end_pattern:
                        need_check_end_pattern = False
                        if end_pattern.search(line):
                            total_boot_num += 1
                            # reset some variables here
                            boot_start = False
                            one_boot_info["Boot_Type"] = boot_type
                            one_boot_info["CPU_Num"], one_boot_info["Disk_Num"] = self.get_cpu_disk_info()
                            one_boot_info["Memory_Size"] = self.get_mem_size()
                            one_boot_info["PCI_Num"] = self.get_pci_num()
                            (one_boot_info["Mtm"], one_boot_info["SN"], one_boot_info["Firmware_Version"], _, _) = \
                                self.get_mtm_fv_sn()

                            if firmware_version:
                                one_boot_info["Firmware_Version"] = firmware_version
                                firmware_version = ""
                            else:
                                one_boot_info["Firmware_Version"] = "Unknown"

                            # add the one boot map to the list here
                            ffdc_boot_list.append(copy.copy(one_boot_info))

                            # reset one boot info here
                            one_boot_info.clear()
                            continue
                        else:
                            # get the related info as usual
                            if re.match(r'(^PEIM\.\w+\.Entry).*~(\d+)ms', line):
                                cp = re.match(r'(^PEIM\.\w+\.Entry).*~(\d+)ms', line).group(1)
                                cp_time = re.match(r'(^PEIM\.\w+\.Entry).*~(\d+)ms', line).group(2)
                                cp_found = True
                            elif re.match(r'(^\w+\.Entry).*~(\d+)ms', line):
                                cp = re.match(r'(^\w+\.Entry).*~(\d+)ms', line).group(1)
                                cp_time = re.match(r'(^\w+\.Entry).*~(\d+)ms', line).group(2)
                                cp_found = True
                            elif re.match(r'(^SMM\.\w+\.Entry).*~(\d+)ms', line):
                                cp = re.match(r'(^SMM\.\w+\.Entry).*~(\d+)ms', line).group(1)
                                cp_time = re.match(r'(^SMM\.\w+\.Entry).*~(\d+)ms', line).group(2)
                                cp_found = True
                            elif re.match(r'(^\[.*\]\.Entry).*~(\d+)ms', line):
                                cp = re.match(r'(^\[.*\]\.Entry).*~(\d+)ms', line).group(1)
                                cp_time = re.match(r'(^\[.*\]\.Entry).*~(\d+)ms', line).group(2)
                                cp_found = True
                            elif re.match(r'(^BDS\.\w+\.Entry).*~(\d+)ms', line):
                                cp = re.match(r'(^BDS\.\w+\.Entry).*~(\d+)ms', line).group(1)
                                cp_time = re.match(r'(^BDS\.\w+\.Entry).*~(\d+)ms', line).group(2)
                                cp_found = True
                            elif re.match(r'(^\w+\.\w+\.Entry).*~(\d+)ms', line):
                                cp = re.match(r'(^\w+\.\w+\.Entry).*~(\d+)ms', line).group(1)
                                cp_time = re.match(r'(^\w+\.\w+\.Entry).*~(\d+)ms', line).group(2)
                                cp_found = True    
                            elif re.match(r'(\w+\.Entry).*~(\d+)ms', line):
                                cp = re.match(r'(\w+\.Entry).*~(\d+)ms', line).group(1)
                                cp_time = re.match(r'(\w+\.Entry).*~(\d+)ms', line).group(2)
                                cp_found = True
                            elif re.match(r'Presented BIOS verson', line):
                                # match line like "LEPT: [GetBiosVersion] Presented BIOS verson: 1.10 IVE114P"
                                firmware_version = line.split(' ')[-2]
                                continue
                            elif re.match(r'Operational\: 0F\:(\d+\.\d+\.\d+\.\d+)', line):
                                firmware_version = re.match(r'Operational\: 0F\:(\d+\.\d+\.\d+\.\d+)', line).group(1)
                                continue
                            elif re.match(r'UcmNotifyBdsStart:AC cycle or BMC reset happened:mUcmFlags', line):
                                boot_type = "AC"
                                continue
                            elif re.match(r'Resetting the platform \(06\)', line):
                                warm_reset_num += 1
                                continue
                            
                            elif re.match(r'System Reset', line):
                                warm_reset_num += 1
                                continue  
                            else:
                                continue

                            if cp_found:
                                cp_found = False
                                if cp in one_boot_info.keys() and one_boot_info[cp]:
                                    one_boot_info[cp] = ":".join(one_boot_info[cp], cp_time)
                                else:
                                    one_boot_info[cp] = cp_time

                    else:
                        # check if need to check end pattern in next line
                        # get the cp and dela time info etc...
                        if "CmdCompleteEvt" in line:
                            need_check_end_pattern = True
                            continue
                        elif re.match(r'(^PEIM\.\w+\.Entry).*~(\d+)ms', line):
                            cp = re.match(r'(^PEIM\.\w+\.Entry).*~(\d+)ms', line).group(1)
                            cp_time = re.match(r'(^PEIM\.\w+\.Entry).*~(\d+)ms', line).group(2)
                            cp_found = True
                        elif re.match(r'(^\w+\.Entry).*~(\d+)ms', line):
                            cp = re.match(r'(^\w+\.Entry).*~(\d+)ms', line).group(1)
                            cp_time = re.match(r'(^\w+\.Entry).*~(\d+)ms', line).group(2)
                            cp_found = True
                        elif re.match(r'(^SMM\.\w+\.Entry).*~(\d+)ms', line):
                            cp = re.match(r'(^SMM\.\w+\.Entry).*~(\d+)ms', line).group(1)
                            cp_time = re.match(r'(^SMM\.\w+\.Entry).*~(\d+)ms', line).group(2)
                            cp_found = True
                        elif re.match(r'(^\[.*\]\.Entry).*~(\d+)ms', line):
                            cp = re.match(r'(^\[.*\]\.Entry).*~(\d+)ms', line).group(1)
                            cp_time = re.match(r'(^\[.*\]\.Entry).*~(\d+)ms', line).group(2)
                            cp_found = True
                        elif re.match(r'(^BDS\.\w+\.Entry).*~(\d+)ms', line):
                            cp = re.match(r'(^BDS\.\w+\.Entry).*~(\d+)ms', line).group(1)
                            cp_time = re.match(r'(^BDS\.\w+\.Entry).*~(\d+)ms', line).group(2)
                            cp_found = True
                        elif re.match(r'(\w+\.Entry).*~(\d+)ms', line):
                                cp = re.match(r'(\w+\.Entry).*~(\d+)ms', line).group(1)
                                cp_time = re.match(r'(\w+\.Entry).*~(\d+)ms', line).group(2)
                                cp_found = True
                        elif re.match(r'Presented BIOS verson', line):
                            # match line like "LEPT: [GetBiosVersion] Presented BIOS verson: 1.10 IVE114P"
                            firmware_version = line.split(' ')[-2]
                            continue
                        elif re.match(r'Operational\: 0F\:(\d+\.\d+\.\d+\.\d+)', line):
                            firmware_version = re.match(r'Operational\: 0F\:(\d+\.\d+\.\d+\.\d+)', line).group(1)
                            continue
                        elif re.match(r'UcmNotifyBdsStart:AC cycle or BMC reset happened:mUcmFlags', line):
                            boot_type = "AC"
                            continue
                        elif re.match(r'Resetting the platform \(06\)', line):
                            warm_reset_num += 1
                            continue
                        else:
                            continue

                        # add the cp and time into map
                        if cp_found:
                            cp_found = False
                            if cp in one_boot_info.keys() and one_boot_info[cp]:
                                one_boot_info[cp] = ":".join([one_boot_info[cp], cp_time])
                            else:
                                one_boot_info[cp] = cp_time
                else:
                    if (("PEIM.InstallPlatformKey.Entry" in line)
                        or ('PowerButtonCallback Entry' in line)
                        or ('Boot Start'in line) or ('PeiLenovoCmosMngr.Entry' in line)):
                        boot_start = True
                    continue
        return total_boot_num, ffdc_boot_list

    def generate_csv_infos_from_uefi(self):
        """
        get boot id, sub bootid, mtm, sn, fw version, cpu/disk/pci/ num, memory size
        boot type, modules names, filename
        """
        global TOTAL_BOOT_NUM
        boot_num = 0
        sub_boot_num = 0

        boot_start, boot_end, need_check_end_pattern = (False, True,False)
        end_pattern = re.compile(r'^\.$')
        firmware_version = "Unknown"
        boot_type, reset_type, boot_mode= ('DC','','Unknown')

        cpu_num, disk_num = self.get_cpu_disk_info()
        memory_size = self.get_mem_size()
        pci_num = self.get_pci_num()
        mtm, sn, last_fw_version, _, _ = self.get_mtm_fv_sn()
        one_boot_info = {}
        firmware_list = []
        ffdc_boot_list = []
        total_ffdc_boot_list = []
        cp_found = False
        cp_found_num = 0
        boot_intact = True
        is_reset = False

        templatePEI = r'(^PEIM\..*\w+.*\.Entry).*~(\d+)ms'
        templateDXE2 = r'(^SMM\..*\w+.*\.Entry).*~(\d+)ms'
        templateDXE3 = r'(^DXE\..*\w+.*\.Entry).*~(\d+)ms'
        templateBDS = r'(^BDS\..*\w+.*\.Entry).*~(\d+)ms'
        templateGeneral = r'(\w+\.Entry).*~(\d+)ms'

        pei_start, dxe_start, bds_start,order = (False, False, False,False)
        reboot = False
        with io.open(self.merged_file, 'r', encoding='ISO-8859-1') as file:
            for line in file:
                line = line.rstrip()
                if boot_start:
                    if need_check_end_pattern:
                        need_check_end_pattern = False
                        if end_pattern.search(line):
                            boot_end = True
                    if dxe_start or bds_start:
                        pei_start = False
                        order = True
                    # check if need to check end pattern in next line
                    # get the cp and dela time info etc...
                    if "CmdCompleteEvt" in line:
                        need_check_end_pattern = True
                        continue
                    elif re.match(templateGeneral, line):
                        cp = re.match(templateGeneral, line).group(1)
                        cp_time = re.match(templateGeneral, line).group(2)
                        cp_found = True
                        bds_start = True
                        continue

                    elif re.match(templatePEI, line):
                        cp = re.match(templatePEI, line).group(1)
                        cp_time = re.match(templatePEI, line).group(2)
                        #if ("PEIM.InstallPlatformKey.Entry" in line) or \
                        #        ("PEIM.LenovoCryptoPpi.Entry" in line) or \
                        #        ("PEIM.PcdPeim.Entry" in line):
                        cp_found = True
                        pei_start = True
                    elif re.match(r'(^\w+\.Entry).*~(\d+)ms', line):
                        cp = re.match(r'(^\w+\.Entry).*~(\d+)ms', line).group(1)
                        cp_time = re.match(r'(^\w+\.Entry).*~(\d+)ms', line).group(2)
                        cp_found = True
                        dxe_start = True
                    elif re.match(templateDXE2, line):
                        cp = re.match(templateDXE2, line).group(1)
                        cp_time = re.match(templateDXE2, line).group(2)
                        cp_found = True
                        dxe_start = True
                    elif re.match(templateDXE3, line):
                        cp = re.match(templateDXE3, line).group(1)
                        cp_time = re.match(templateDXE3, line).group(2)
                        cp_found = True
                        dxe_start = True
                    elif re.match(r'(^\[.*\]\.Entry).*~(\d+)ms', line):
                        cp = re.match(r'(^\[.*\]\.Entry).*~(\d+)ms', line).group(1)
                        cp_time = re.match(r'(^\[.*\]\.Entry).*~(\d+)ms', line).group(2)
                        cp_found = True
                        dxe_start = True
                    elif re.match(templateBDS, line):
                        cp = re.match(templateBDS, line).group(1)
                        cp_time = re.match(templateBDS, line).group(2)
                        cp_found = True
                        if 'Dxe' in line:
                            dxe_start = True
                        elif 'Pei' in line:
                            pei_start = True
                    
                   
                    #elif re.match(r'Operational\: 0F\:(\d+\.\d+\.\d+\.\d+)', line):
                        # firmware_version = re.match(r'Operational\: 0F\:(\d+\.\d+\.\d+\.\d+)', line).group(1)
                        #continue
                    elif re.match(r'Presented BIOS verson', line):
                        # match line like "LEPT: [GetBiosVersion] Presented BIOS verson: 1.10 IVE114P"
                        #firmware_version = line.split(' ')[-2]
                        firmware_version = 'firm7'
                        continue
                    
                    elif re.match(r'UcmNotifyBdsStart:AC cycle or BMC reset happened:mUcmFlags', line):
                        boot_type = "AC"
                        continue
                    # add boot to  Setup, OS or Shell
                    elif 'Progress Code 0xC  sent to FPGA' in line or \
                            'Progress Code 0x0C  sent to FPGA' in line:
                        boot_end = True
                    # elif 'Progress Code 0x18  sent to FPGA' in line or \
                    #         'Progress Code 0x14  sent to FPGA' in line:
                    #     boot_end = True
                    elif 'ShellFull.Entry' in line or 'Shell.Entry' in line:
                        boot_end = True
                    elif '[LIMIT_BOOT] Boot Fail.' in line :
                        boot_end = True
                        reboot = True
                    elif  "System Reset" in line:
                        reboot = True
                    elif "UEFI_OS_BOOTED" in line:
                        boot_end = True
                        reboot = False
                    elif 'PeiLenovoCmosMngrEntryPoint: second pass' in line:
                        boot_end = False
                        reboot = True
                        is_reset = True
                        continue
                    elif 'Boot UEFI' in line:
                        boot_end = True
                        reboot = False
                    elif re.match(r'Resetting the platform \(06\)', line):
                        if not boot_end:
                            reset_type = "Warm"
                            is_reset = True
                            sub_boot_num += 1
                    elif re.match(r'PowerButtonCallback Entry', line) or re.match(
                            'Resetting the platform \(0E\)', line):
                        if not boot_end:
                            reset_type = "Cold"
                            is_reset = True
                            sub_boot_num += 1
                    elif re.match(r'LenovoBootModeData.SystemBootMode \d',line):
                        # LenovoBootModeData.SystemBootMode 0
                        boot_mode = line.split(r' ')[-1]
                    #else:
                        #continue
                    if not boot_end or not is_reset:
                        if (('UEFI BOOT START:'in line) or ('Boot Start'in line) or ('PeiLenovoCmosMngr.Entry' in line)):
                            boot_intact = False
                            boot_start = False
                            sub_boot_num = 0
                            cp_found_num = 0
                            TOTAL_BOOT_NUM += 1
                            boot_num += 1
                            firmware_list.append(firmware_version)
                            # handle boot type
                            for m in ffdc_boot_list:
                                m['Intact'] = 'N'
                                m['Boot_ID'] = TOTAL_BOOT_NUM
                                if boot_type == 'AC' and m['Sub_Boot_ID'] == 1:
                                    m['Boot_Type'] = boot_type
                            total_ffdc_boot_list.extend(ffdc_boot_list)
                            ffdc_boot_list = []
                            boot_type = "DC"
                            reset_type = ""
                            pei_start, dxe_start, bds_start, order = (False, False, False, False)
                            continue
                        if order and pei_start:
                            bds_start = False
                            dxe_start = False
                            order = False
                            #new boot
                            TOTAL_BOOT_NUM += 1
                            boot_num += 1
                            firmware_list.append(firmware_version)
                            # handle boot type
                            for m in ffdc_boot_list:
                                m['Intact'] = 'N'
                                m['Boot_ID'] = TOTAL_BOOT_NUM
                                if boot_type == 'AC' and m['Sub_Boot_ID'] == 1:
                                    m['Boot_Type'] = boot_type
                            total_ffdc_boot_list.extend(ffdc_boot_list)
                            ffdc_boot_list = []
                            boot_type = "DC"
                            reset_type = ""
                            sub_boot_num = 1
                            cp_found_num = 0
                    if cp_found:
                        cp_found = False
                        cp_found_num += 1
                        one_boot_info['Module'] = cp
                        one_boot_info['Time'] = cp_time
                        one_boot_info['Boot_Type'] = reset_type if reset_type else boot_type
                        one_boot_info['Sub_Boot_ID'] = sub_boot_num
                        #one_boot_info['Boot_ID'] = TOTAL_BOOT_NUM

                        ffdc_boot_list.append(copy.copy(one_boot_info))
                        if is_reset:
                            is_reset = False
                            bds_start = False
                            dxe_start = False
                            order = False

                    if boot_end:
                        boot_intact = True
                        boot_start = False
                        sub_boot_num = 0
                        cp_found_num = 0
                        TOTAL_BOOT_NUM += 1
                        boot_num += 1
                        firmware_list.append(firmware_version)
                        # handle boot type
                        Intact_flag = 'N' if reboot else 'Y'
                        for m in ffdc_boot_list:
                            m['Intact'] = Intact_flag
                            m['Boot_ID'] = TOTAL_BOOT_NUM
                            if boot_type == 'AC' and m['Sub_Boot_ID'] == 1:
                                m['Boot_Type'] = boot_type
                        total_ffdc_boot_list.extend(ffdc_boot_list)
                        ffdc_boot_list = []
                        boot_type = "DC"
                        reset_type = ""
                        pei_start, dxe_start, bds_start,order = (False, False, False,False)
                        reboot = False
                else:
                    m = ((("PEIM.InstallPlatformKey.Entry" in line)
                        or ("PEIM.LenovoCryptoPpi.Entry" in line)
                        or ("PEIM.PcdPeim.Entry" in line)) \
                            and re.match(r'(^PEIM\.\w+\.Entry).*~(\d+)ms', line))
                    n = (('UEFI BOOT START:' in line) 
                        or ('Boot Start' in line) or ('PeiLenovoCmosMngr.Entry' in line))
                    if m or n:
                        boot_start = True
                        boot_end = False
                        boot_intact = False
                        #TOTAL_BOOT_NUM += 1
                        #boot_num += 1
                        sub_boot_num += 1
                        if re.match(r'(^PEIM\.\w+\.Entry).*~(\d+)ms', line):
                            cp = re.match(r'(^PEIM\.\w+\.Entry).*~(\d+)ms', line).group(1)
                            cp_time = re.match(r'(^PEIM\.\w+\.Entry).*~(\d+)ms', line).group(2)
                            if int(cp_time) > 10000:
                                boot_type = "Warm"
                            one_boot_info['Module'] = cp
                            one_boot_info['Time'] = cp_time
                            one_boot_info['Boot_Type'] = reset_type if reset_type else boot_type
                            one_boot_info['Sub_Boot_ID'] = sub_boot_num
                            one_boot_info['Intact'] = 'N'
                            #one_boot_info['Boot_ID'] = TOTAL_BOOT_NUM

                            ffdc_boot_list.append(copy.copy(one_boot_info))
                    elif re.match(r'Resetting the platform \(06\)', line):
                        boot_type = "Warm"
                    elif re.match(r'PowerButtonCallback Entry', line) or re.match(
                            'Resetting the platform \(0E\)', line):
                        boot_type = "Cold"
            # the last boot segment
            if not boot_intact and cp_found_num:
                print("find last no intact boot segment!")
                TOTAL_BOOT_NUM += 1
                boot_num += 1
                firmware_list.append(firmware_version)
                for m in ffdc_boot_list:
                    m['Intact'] = 'N'
                    m['Boot_ID'] = TOTAL_BOOT_NUM
                    if boot_type == 'AC' and m['Sub_Boot_ID'] == 1:
                        m['Boot_Type'] = boot_type
                total_ffdc_boot_list.extend(ffdc_boot_list)

        # update last firmware version
        if len(firmware_list) == boot_num and len(firmware_list):
            firmware_list[-1] = last_fw_version
            
        else:
            firmware_list.append(last_fw_version)
            

        return (boot_num, cpu_num, disk_num, memory_size, pci_num, mtm, sn, boot_mode, firmware_list, total_ffdc_boot_list)

    def generate_csv_infos_from_uefi_v1(self):
        """
        get boot id, sub bootid, mtm, sn, fw version, cpu/disk/pci/ num, memory size
        boot type, modules names, filename
        """
        global TOTAL_BOOT_NUM
        boot_num = 0
        sub_boot_num = 0

        boot_start, need_check_end_pattern = (False, False)
        end_pattern = re.compile(r'^\.$')
        firmware_version = "Unknown"
        boot_type, reset_type, boot_mode = ('DC','','Unknown')

        cpu_num, disk_num = self.get_cpu_disk_info()
        memory_size = self.get_mem_size()
        pci_num = self.get_pci_num()
        mtm, sn, last_fw_version, _, _ = self.get_mtm_fv_sn()
        one_boot_info = {}
        firmware_list = []
        ffdc_boot_list = []
        total_ffdc_boot_list = []
        cp_found = False
        cp_found_num = 0

        with io.open(self.merged_file, 'r', encoding='ISO-8859-1') as file:
            for line in file:
                line = line.rstrip()
                if boot_start:
                    if need_check_end_pattern:
                        need_check_end_pattern = False
                        if end_pattern.search(line):
                            boot_start = False
                            sub_boot_num = 0
                            cp_found_num = 0
                            TOTAL_BOOT_NUM += 1
                            boot_num += 1
                            firmware_list.append(firmware_version)
                            # handle boot type
                            for m in ffdc_boot_list:
                                m['Intact'] = 'Y'
                                m['Boot_ID'] = TOTAL_BOOT_NUM
                                if boot_type == 'AC' and m['Sub_Boot_ID'] == 1:
                                    m['Boot_Type'] = boot_type

                            total_ffdc_boot_list.extend(ffdc_boot_list)
                            ffdc_boot_list = []
                            continue
                    # check if need to check end pattern in next line
                    # get the cp and dela time info etc...
                    if "CmdCompleteEvt" in line:
                        need_check_end_pattern = True
                        continue
                    elif re.match(r'(^PEIM\.\w+\.Entry).*~(\d+)ms', line):
                        cp = re.match(r'(^PEIM\.\w+\.Entry).*~(\d+)ms', line).group(1)
                        cp_time = re.match(r'(^PEIM\.\w+\.Entry).*~(\d+)ms', line).group(2)
                        #if ("PEIM.InstallPlatformKey.Entry" in line) or \
                        #        ("PEIM.LenovoCryptoPpi.Entry" in line) or \
                        #        ("PEIM.PcdPeim.Entry" in line):
                        # handle some abnormal cases
                        if "PEIM.InstallPlatformKey.Entry" in line and int(cp_time) < 2000 \
                                and cp_found_num > 5:
                            reset_type = "Cold"
                            TOTAL_BOOT_NUM += 1
                            boot_num += 1
                            firmware_list.append(firmware_version)
                            # handle boot type
                            for m in ffdc_boot_list:
                                m['Intact'] = 'N'
                                m['Boot_ID'] = TOTAL_BOOT_NUM
                                if boot_type == 'AC' and m['Sub_Boot_ID'] == 1:
                                    m['Boot_Type'] = boot_type

                            total_ffdc_boot_list.extend(ffdc_boot_list)
                            ffdc_boot_list = []
                            sub_boot_num = 1
                            cp_found_num = 0
                        cp_found = True
                    elif re.match(r'(^\w+\.Entry).*~(\d+)ms', line):
                        cp = re.match(r'(^\w+\.Entry).*~(\d+)ms', line).group(1)
                        cp_time = re.match(r'(^\w+\.Entry).*~(\d+)ms', line).group(2)
                        cp_found = True
                    elif re.match(r'(^SMM\.\w+\.Entry).*~(\d+)ms', line):
                        cp = re.match(r'(^SMM\.\w+\.Entry).*~(\d+)ms', line).group(1)
                        cp_time = re.match(r'(^SMM\.\w+\.Entry).*~(\d+)ms', line).group(2)
                        cp_found = True
                    elif re.match(r'(^\[.*\]\.Entry).*~(\d+)ms', line):
                        cp = re.match(r'(^\[.*\]\.Entry).*~(\d+)ms', line).group(1)
                        cp_time = re.match(r'(^\[.*\]\.Entry).*~(\d+)ms', line).group(2)
                        cp_found = True
                    elif re.match(r'(^BDS\.\w+\.Entry).*~(\d+)ms', line):
                        cp = re.match(r'(^BDS\.\w+\.Entry).*~(\d+)ms', line).group(1)
                        cp_time = re.match(r'(^BDS\.\w+\.Entry).*~(\d+)ms', line).group(2)
                        cp_found = True
                    
                    elif re.match(r'(\w+\.Entry).*~(\d+)ms', line):
                        cp = re.match(r'(\w+\.Entry).*~(\d+)ms', line).group(1)
                        cp_time = re.match(r'(\w+\.Entry).*~(\d+)ms', line).group(2)
                        cp_found = True
                        
                    elif re.match(r'Operational\: 0F\:(\d+\.\d+\.\d+\.\d+)', line):
                        firmware_version = re.match(r'Operational\: 0F\:(\d+\.\d+\.\d+\.\d+)', line).group(1)
                        
                        continue
                    elif re.match(r'Presented BIOS verson', line):
                        # match line like "LEPT: [GetBiosVersion] Presented BIOS verson: 1.10 IVE114P"
                        firmware_version = line.split(' ')[-2]
                        continue
                    elif re.match(r'UcmNotifyBdsStart:AC cycle or BMC reset happened:mUcmFlags', line):
                        boot_type = "AC"
                        continue
                    elif re.match(r'Resetting the platform \(06\)', line):
                        reset_type = "Warm"
                        sub_boot_num += 1
                    elif re.match(r'PowerButtonCallback Entry', line) or re.match(
                            r'Resetting the platform \(0E\)', line):
                        # treat the cold reboot as new boot
                        reset_type = "Cold"
                        boot_start = False
                        TOTAL_BOOT_NUM += 1
                        boot_num += 1
                        firmware_list.append(firmware_version)
                        # handle boot type
                        for m in ffdc_boot_list:
                            m['Intact'] = "N"
                            m['Boot_ID'] = TOTAL_BOOT_NUM
                            if boot_type == 'AC' and m['Sub_Boot_ID'] == 1:
                                m['Boot_Type'] = boot_type

                        total_ffdc_boot_list.extend(ffdc_boot_list)
                        ffdc_boot_list = []
                        sub_boot_num = 0
                        cp_found_num = 0
                        continue
                    elif re.match(r'LenovoBootModeData.SystemBootMode \d',line):
                        # LenovoBootModeData.SystemBootMode 0
                        boot_mode = line.split(' ')[-1]
                    else:
                        continue

                    if cp_found:
                        cp_found = False
                        cp_found_num += 1
                        one_boot_info['Module'] = cp
                        one_boot_info['Time'] = cp_time
                        one_boot_info['Boot_Type'] = reset_type if reset_type else boot_type
                        one_boot_info['Sub_Boot_ID'] = sub_boot_num
                        #one_boot_info['Boot_ID'] = TOTAL_BOOT_NUM

                        ffdc_boot_list.append(copy.copy(one_boot_info))
                else:
                    if ("PEIM.InstallPlatformKey.Entry" in line) or \
                            ("PEIM.LenovoCryptoPpi.Entry" in line) or \
                            ("PEIM.PcdPeim.Entry" in line) or \
                                ('PeiLenovoCmosMngr.Entry' in line):
                        boot_start = True
                        #TOTAL_BOOT_NUM += 1
                        #boot_num += 1
                        sub_boot_num += 1
                        if re.match(r'(^PEIM\.\w+\.Entry).*~(\d+)ms', line):
                            cp = re.match(r'(^PEIM\.\w+\.Entry).*~(\d+)ms', line).group(1)
                            cp_time = re.match(r'(^PEIM\.\w+\.Entry).*~(\d+)ms', line).group(2)
                            one_boot_info['Module'] = cp
                            one_boot_info['Time'] = cp_time
                            one_boot_info['Boot_Type'] = reset_type if reset_type else boot_type
                            one_boot_info['Sub_Boot_ID'] = sub_boot_num
                            #one_boot_info['Boot_ID'] = TOTAL_BOOT_NUM

                            ffdc_boot_list.append(copy.copy(one_boot_info))

        # update last firmware version
        if len(firmware_list) == boot_num and len(firmware_list):
            firmware_list[-1] = last_fw_version
            
        else:
            firmware_list.append(last_fw_version)
           

        return (boot_num, cpu_num, disk_num, memory_size, pci_num, mtm, sn, boot_mode, firmware_list, total_ffdc_boot_list)

    def get_offset(self):
        zone_patten = re.compile(r'^   (\d{2}/\d{2}/\d{4} \d{2}:\d{2}:\d{2}) UTC ([+|-]\d{2}:\d{2}) dst (\w+)')
        service_path = os.path.join(self.uncompress_temp_dir, "tmp", "service.txt")
        offset = 0
        with open(service_path, 'rt') as file:
            for line in file:
                line = line.rstrip()
                m = zone_patten.match(line)
                if m:
                    time_zone = m.group(2)
                    dst_flag = 0 if m.group(3) == 'off' else 1
                    abs_offset = int(time_zone.split(':')[0][1:]) + dst_flag
                    offset = (0 + abs_offset) if time_zone[0] == '+' else (0 - abs_offset)
                    break
        return offset

    def generate_boot_infos_from_uefi(self, uefi_file=None):
        """
        get boot id, sub bootid, mtm, sn, fw version, cpu/disk/pci/ num, memory size
        boot type, modules names, filename
        """
        # global TOTAL_BOOT_NUM
        boot_num = 0
        sub_boot_num = 0
        boot_start, boot_end, boot_break = (False, True, False)
        need_check_end_pattern = False
        end_pattern = re.compile(r'^\.$')
        firmware_version = "Unknown"
        boot_type, reset_type, boot_mode = ('DC', '', 'Unknown')

        cpu_num, disk_num = self.get_cpu_disk_info()
        memory_size = self.get_mem_size()
        pci_num = self.get_pci_num()
        mtm, sn, last_fw_version, _, uuid = self.get_mtm_fv_sn()
        zone_offset = self.get_offset()

        one_boot_info = {}
        firmware_list = []
        ffdc_boot_list = []
        total_ffdc_boot_list = []
        boot_intact = True
        is_reset = False
        is_module = False
        from datetime import datetime  #wanghx add
        start_time = self.first_shift_time
        #self.first_shift_time

        templatePEI = r'(^PEIM\.\w+\.Entry).*~(\d+)ms'
        templateDXE = r'(^\w+\.Entry).*~(\d+)ms|(^SMM\.\w+\.Entry).*~(\d+)ms|(^\[.*\]\.Entry).*~(\d+)ms'
        templateDXE3 = r'(^DXE\..*\w+\.Entry).*~(\d+)ms'
        templateBDS = r'(^BDS\.\w+\.Entry).*~(\d+)ms'
        templateGeneral = r'(\w+\.Entry).*~(\d+)ms'
        phase = ''
        LOG_SHIFT_LINE = re.compile(
            r'===>> log shift at (\w{3} \w{3} *\d{1,2} \d{2}:\d{2}:\d{2} \d{4})') # sometimes multiple Spaces before months

        LOG_SHIFT_LINE_NEW = re.compile(r'log shift at (\w{3} \w{3} *\d{1,2} \d{2}:\d{2}:\d{2} \d{4})')
        BOOT_START_2 = re.compile(
            r'^UEFI BOOT START: (\d{4}Y-\d{2}M-\d{2}D \d{2}:\d{2}:\d{2})')
        Boot_Start = re.compile(
            r'Boot Start: (\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2})')
        Boot_Start2 =  re.compile(
            r'\[(\d{7})\s+(\d{1,2}:\d{2}:\d{2})\s+TSC = 0x[0-9A-F]+\]')





        if uefi_file:
            target_file = uefi_file
        else:
            target_file = self.merged_file

        pei_start, dxe_start, bds_start = (False, False, False)
        reboot = False

        with io.open(target_file, 'r', encoding='ISO-8859-1') as lines:
            for line in lines:
                line = line.strip()
                if boot_start:

                    m0 = LOG_SHIFT_LINE.match(line)
                    m0_new = LOG_SHIFT_LINE_NEW.findall(line)
                    m0_new2 = Boot_Start.findall(line)
                    m0_new3 = Boot_Start2.findall(line)
                    if m0:
                        start_time = (datetime.strptime(m0.group(1), "%a %b %d %H:%M:%S %Y")
                                      + timedelta(hours=zone_offset)).strftime('%Y-%m-%d %H:%M:%S')
                        continue
            
                        
                    elif m0_new2:
                        #start_time= m0_new2.group(1)
                        start_time = m0_new2[0]
                        continue

                    elif m0_new3:
                        date = m0_new3[0][0]
                        time = m0_new3[0][1]
                        start_time = f"{date[:4]}/{date[4:6]}/{date[6:]}  {time}"
                        continue
                    
                    elif m0_new:
                        start_time = (datetime.strptime(m0_new[0], "%a %b %d %H:%M:%S %Y")
                                      + timedelta(hours=zone_offset)).strftime('%Y-%m-%d %H:%M:%S')
                        continue
                    

                    one_boot_info['Log'] = line
                    one_boot_info['Sub_Boot_ID'] = sub_boot_num
                    one_boot_info['Start_time'] = start_time
                    one_boot_info['Boot_Type'] = reset_type if reset_type else boot_type


                    if re.match(templatePEI, line) or "Resetting the platform (06)" in line:
                        phase = 'PEI'
                    #elif re.match(templateDXE, line) and not bds_start:
                    #    phase = 'DXE1'
                    #elif re.match(templateDXE, line) and pei_start:
                    #    phase = 'DXE2'
                    elif re.match(templateDXE3, line) and not bds_start:
                        phase = 'DXE'
                    elif re.match(templateDXE3, line) and pei_start:
                        phase = 'DXE'
                    elif re.match(templateBDS, line):
                        phase = 'BDS'
                    #elif dxe_start and not pei_start and not bds_start:
                    #    phase = 'DXE5'
                    #elif bds_start and not dxe_start and not pei_start:
                    #    phase = 'BDS2'
                    elif re.match(templateGeneral, line) and 'Pei' in line:
                        phase = 'PEI'
                    elif re.match(templateGeneral, line) and 'Dxe' in line:
                        phase = 'DXE'
                    else:
                        phase='unknown'

                    one_boot_info['Phase'] = phase

                    templatemodule = templatePEI + '|' + templateDXE3 + '|' + templateBDS + '|' + templateDXE + '|' + templateGeneral
                    one_boot_info['Is_module'] = 'Y' if re.match(templatemodule, line) else 'N'

                    ffdc_boot_list.append(copy.copy(one_boot_info))

                    if re.match(r'Operational\: 0F\:(\d+\.\d+\.\d+\.\d+)', line):
                        firmware_version = re.match(r'Operational\: 0F\:(\d+\.\d+\.\d+\.\d+)', line).group(1)
                        

                        
                    # match lines like "LEPT: [GetBiosVersion] Presented BIOS verson: 1.10 IVE114P"
                    elif re.match(r'Presented BIOS verson', line):
                        firmware_version = line.split(' ')[-2]
                        

                    # match the AC tag
                    elif re.match(r'UcmNotifyBdsStart:AC cycle or BMC reset happened:mUcmFlags', line):
                        boot_type = "AC"

                    # match the tags of boot ending: going to Setup, OS or Shell
                    elif 'Progress Code 0xC' in line or 'Progress Code 0x0C' in line:
                        boot_end = True
                    # elif 'Progress Code 0x18' in line or 'Progress Code 0x14' in line: # wanghx replace it by CmdCompleteEvt
                    #     boot_end = True
                    elif 'ShellFull.Entry' in line or 'Shell.Entry' in line:
                        boot_end = True
                    elif "CmdCompleteEvt" in line: #wanghx add
                        need_check_end_pattern = True
                        continue
                    elif need_check_end_pattern:
                        need_check_end_pattern = False
                        if end_pattern.search(line):
                            boot_end = True

                    elif '[LIMIT_BOOT] Boot Fail.' in line:
                        boot_end = True
                        reboot = True
                    elif 'UEFI_OS_BOOTED' in line :
                        boot_end = True
                    elif 'Boot UEFI' in line:
                        boot_end = True
                    elif 'reboot:1' in line:
                        reboot=True
                        boot_end = True

                    # split new sub boot
                    elif re.match(r'Resetting the platform \(06\)', line):
                        if not boot_end:
                            reset_type = "Warm"
                            is_reset = True
                            sub_boot_num += 1
                    elif re.match(r'PowerButtonCallback Entry', line) or re.match(
                            r'Resetting the platform \(0E\)', line):
                        if not boot_end:
                            reset_type = "Cold"
                            is_reset = True
                            sub_boot_num += 1

                    # match lines like: "LenovoBootModeData.SystemBootMode 0"
                    elif re.match(r'LenovoBootModeData.SystemBootMode \d', line):
                        boot_mode = line.split(' ')[-1]

                    elif ('UEFI BOOT START:' in line) or ('Boot Start' in line):
                        m1 = BOOT_START_2.match(line)
                        m2 = Boot_Start.search(line)
                        if m1:
                            try:
                                start_time = (datetime.strptime(m1.group(1), "%YY-%mM-%dD %H:%M:%S") \
                                    + timedelta(hours=zone_offset)).strftime('%Y-%m-%d %H:%M:%S')
                                
                            except Exception as e:
                                print(m1.group(1), 'is not correct format')
                        # struct_time = time.strptime(time1, "%YY-%mM-%dD %H:%M:%S").strftime('%Y-%m-%d %H:%M:%S')
                        # s = time.mktime(struct_time)
                        # start_time = datetime.fromtimestamp(s)
                        elif m2:
                            start_time = m2.group(1)
                            
                        is_module = False

                    if re.match(templatePEI, line):
                        # if last subboot have bds phase, when UEFI BOOT START occur, is_reset is true, can not identify new boot
                        # SR850P_7D2GCTO1WW_LNVSDVU002_xcc_191104-160445.tzz, 3300ms,11,3
                        if is_reset and bds_start and not pei_start:
                            is_reset = False
                            bds_start = False
                        pei_start = True
                        is_module = True
                        # if subboot only have pei phase, when UEFI BOOT START occur, it not correctly parsed new boot
                        # 7X1925Z000_LNVSDV8803_xcc_190611-095049.tzz, 2458ms, 18,1
                        if not dxe_start and not bds_start and is_reset:
                            is_reset = False
                    elif re.match(templateDXE, line):
                        pei_start = False
                        dxe_start = True
                        is_reset = False
                        is_module = True

                    elif re.match(templateDXE3, line):
                        pei_start = False
                        dxe_start = True
                        is_reset = False
                        is_module = True
                    elif re.match(templateBDS, line):
                        # 7X16CTO1WW_S4ADG572_xcc_200225-090754.tzz, 10.241.54.122 no dxe phase, 52376ms, identify boot error
                        pei_start = False
                        dxe_start = False
                        bds_start = True
                        is_reset = False
                        is_module = True


                    # PEI -> DXE -> not ending but PEI is started again
                    if not boot_end and not is_reset and dxe_start and pei_start:
                        boot_break = True
                    # PEI -> DXE -> BDS -> not ending but PEI is started again
                    if not boot_end and not is_reset and bds_start and pei_start:
                        boot_break = True
                    # not ending but new boot is started again
                    if not boot_end and not is_reset and ('UEFI BOOT START:' or 'Boot Start:' or 'PeiLenovoCmosMngr.Entry') in line :
                        boot_break = True

                    # 7X02CTO1WW_J300850G_xcc_190328-231118.tzz,1961ms,4,1, last boot only has pei, and the new boot has no EFI BOOT START
                    if not boot_end and not is_reset and \
                    ((("PEIM.InstallPlatformKey.Entry" in line)
                    or ("PEIM.LenovoCryptoPpi.Entry" in line)
                    or ("PEIM.PcdPeim.Entry" in line)) \
                    and re.match(r'(^PEIM\.\w+\.Entry).*~(\d+)ms', line)) \
                    and len(ffdc_boot_list) > 10 and sub_boot_num == 1 and int(line.split('~')[1].split('ms')[0]) < module_time:
                        boot_break = True
                    
                    if not boot_end and not is_reset and ('PeiLenovoCmosMngr.Entry' in line) and len(ffdc_boot_list) > 10 and sub_boot_num == 1 and int(line.split('~')[1].split('ms')[0]) < module_time:
                        boot_break = True 

                    if re.match(templatemodule, line):
                        module_time = int(line.split('~')[1].split('ms')[0])

                    # the current boot is broken
                    if boot_break:
                        boot_intact = False
                        firmware_list.append(firmware_version)
                        for m in ffdc_boot_list[:-1]:
                            m['Intact'] = 'N'
                            if boot_type == 'AC' and m['Sub_Boot_ID'] == 1:
                                m['Boot_Type'] = boot_type

                        total_ffdc_boot_list.extend(ffdc_boot_list[:-1])

                        if pei_start or 'UEFI BOOT START:' or 'Boot Start:' in line: #wanghx add
                            phase = 'PEI'
                        elif not pei_start and dxe_start:
                           phase = 'DXE'
                        elif not dxe_start and bds_start:
                           phase = 'BDS'
                        one_boot_info['Phase'] = phase

                        boot_num += 1
                        sub_boot_num = 1
                        boot_start = True
                        boot_break = False
                        is_reset = False
                        pei_start = True
                        dxe_start = False
                        bds_start = False
                        ffdc_boot_list = []
                        boot_type = "DC"
                        reset_type = ""
                        if re.match(templatePEI, line):
                            cp_time = re.match(templatePEI, line).group(2)
                            if int(cp_time) > 10000:
                                boot_type = "Warm"
                        one_boot_info['Log'] = line
                        one_boot_info['Boot_Type'] = boot_type
                        one_boot_info['Boot_ID'] = boot_num
                        one_boot_info['Sub_Boot_ID'] = sub_boot_num
                        one_boot_info['Intact'] = 'N'
                        one_boot_info['Is_module'] = 'Y' if is_module else 'N'
                        one_boot_info['Start_time'] = start_time

                        ffdc_boot_list.append(copy.copy(one_boot_info))
                        is_module = False
                        continue

                    # boot ending
                    if boot_end:
                        boot_intact = True
                        boot_start = False
                        boot_break = False
                        is_reset = False
                        pei_start = False
                        dxe_start = False
                        bds_start = False
                        sub_boot_num = 0
                        firmware_list.append(firmware_version)

                        # handle boot type
                        Intact_flag = 'N' if reboot else 'Y'
                        for m in ffdc_boot_list:
                            m['Intact'] = Intact_flag
                            if boot_type == 'AC' and m['Sub_Boot_ID'] == 1:
                                m['Boot_Type'] = boot_type

                        total_ffdc_boot_list.extend(ffdc_boot_list)
                        ffdc_boot_list = []
                        boot_type = "DC"
                        reset_type = ""
                        reboot = False

                else:
                    m = ((("PEIM.InstallPlatformKey.Entry" in line)
                        or ("PEIM.LenovoCryptoPpi.Entry" in line)
                        or ("PEIM.PcdPeim.Entry" in line)) \
                            and re.match(r'(^PEIM\.\w+\.Entry).*~(\d+)ms', line))
                    n = (('UEFI BOOT START:' in line) or ("Boot Start:" in line) or (('PeiLenovoCmosMngr.Entry' in line)))
                    if m or n:
                        boot_start = True
                        boot_end = False
                        boot_intact = False
                        is_module = True
                        # TOTAL_BOOT_NUM += 1
                        boot_num += 1
                        sub_boot_num += 1
                        phase = 'PEI'

                        # cp = re.match(r'(^PEIM\.\w+\.Entry).*~(\d+)ms', line).group(1)
                        if m:
                            is_module = True
                            cp_time = re.match(r'(^PEIM\.\w+\.Entry).*~(\d+)ms', line).group(2)
                            if int(cp_time) > 10000:
                                boot_type = "Warm"
                        elif n:
                            is_module = False
                            n1 = BOOT_START_2.match(line)
                            n2 = Boot_Start.match(line)
                            if n1:
                                try:
                                    start_time = (datetime.strptime(n1.group(1), "%YY-%mM-%dD %H:%M:%S")\
                                         + timedelta(hours=zone_offset)).strftime('%Y-%m-%d %H:%M:%S')
                                    
                                except Exception as e:
                                    print(n1.group(1), 'is not correct format')
                            elif n2 :
                                start_time = n2.group(1)
                                

                        one_boot_info['Log'] = line
                        one_boot_info['Boot_Type'] = reset_type if reset_type else boot_type
                        one_boot_info['Boot_ID'] = boot_num
                        one_boot_info['Sub_Boot_ID'] = sub_boot_num
                        one_boot_info['Intact'] = 'N'

                        one_boot_info['Phase'] = phase #wanghx add
                        one_boot_info['Is_module'] = 'Y' if is_module else 'N'
                        one_boot_info['Start_time'] = start_time
                        is_module = False

                        ffdc_boot_list.append(copy.copy(one_boot_info))
                    elif re.match(r'Resetting the platform \(06\)', line):
                        boot_type = "Warm"
                    elif re.match(r'PowerButtonCallback Entry', line) or \
                            re.match(r'Resetting the platform \(0E\)', line):
                        boot_type = "Cold"

            # the last boot segment
            if not boot_intact:
                firmware_list.append(firmware_version)
                for m in ffdc_boot_list:
                    m['Intact'] = 'N'
                    if boot_type == 'AC' and m['Sub_Boot_ID'] == 1:
                        m['Boot_Type'] = boot_type
                total_ffdc_boot_list.extend(ffdc_boot_list)

        # update last firmware version
        if len(firmware_list) == boot_num and len(firmware_list):
            firmware_list[-1] = last_fw_version
            
        else:
            firmware_list.append(last_fw_version)
            

        return (
        boot_num, cpu_num, disk_num, memory_size, pci_num, mtm, sn, boot_mode, firmware_list, total_ffdc_boot_list, uuid)

    def generate_boot_infos_from_uefi_v1(self, uefi_file=None):
        """
        get boot id, sub bootid, mtm, sn, fw version, cpu/disk/pci/ num, memory size
        boot type, modules names, filename
        """
        global TOTAL_BOOT_NUM
        boot_num = 0
        sub_boot_num = 0
        boot_start, need_check_end_pattern = (False, False)
        end_pattern = re.compile(r'^\.$')
        firmware_version = "Unknown"
        boot_type, reset_type, boot_mode = ('DC', '', 'Unknown')

        cpu_num, disk_num = self.get_cpu_disk_info()
        memory_size = self.get_mem_size()
        pci_num = self.get_pci_num()
        mtm, sn, last_fw_version, _, _ = self.get_mtm_fv_sn()
        one_boot_info = {}
        firmware_list = []
        ffdc_boot_list = []
        total_ffdc_boot_list = []
        cp_found = False
        cp_found_num = 0

        if uefi_file:
            target_file = uefi_file
        else:
            target_file = self.merged_file
        with io.open(target_file, 'r', encoding='ISO-8859-1') as lines:
            for line in lines:
                line = line.strip()
                if boot_start:
                    if need_check_end_pattern:
                        need_check_end_pattern = False

                        one_boot_info['Log'] = line
                        ffdc_boot_list.append(copy.copy(one_boot_info))

                        if end_pattern.search(line):
                            boot_start = False
                            sub_boot_num = 0
                            cp_found_num = 0
                            TOTAL_BOOT_NUM += 1
                            boot_num += 1
                            firmware_list.append(firmware_version)
                            # handle boot type
                            for m in ffdc_boot_list:
                                m['Intact'] = 'Y'
                                m['Boot_ID'] = boot_num
                                if boot_type == 'AC' and m['Sub_Boot_ID'] == 1:
                                    m['Boot_Type'] = boot_type

                            total_ffdc_boot_list.extend(ffdc_boot_list)
                            ffdc_boot_list = []
                            continue
                    # check if need to check end pattern in next line
                    # get the cp and dela time info etc...
                    if "CmdCompleteEvt" in line:
                        need_check_end_pattern = True
                        # continue
                    elif re.match(r'(^PEIM\.\w+\.Entry).*~(\d+)ms', line):
                        # cp = re.match(r'(^PEIM\.\w+\.Entry).*~(\d+)ms', line).group(1)
                        cp_time = re.match(r'(^PEIM\.\w+\.Entry).*~(\d+)ms', line).group(2)

                        # if ("PEIM.InstallPlatformKey.Entry" in line) or \
                        #        ("PEIM.LenovoCryptoPpi.Entry" in line) or \
                        #        ("PEIM.PcdPeim.Entry" in line):

                        # handle some abnormal cases
                        if "PEIM.InstallPlatformKey.Entry" in line and int(cp_time) < 2000 \
                                and cp_found_num > 5:
                            reset_type = "Cold"
                            TOTAL_BOOT_NUM += 1
                            boot_num += 1
                            firmware_list.append(firmware_version)
                            # handle boot type
                            for m in ffdc_boot_list:
                                m['Intact'] = 'N'
                                m['Boot_ID'] = boot_num
                                if boot_type == 'AC' and m['Sub_Boot_ID'] == 1:
                                    m['Boot_Type'] = boot_type

                            total_ffdc_boot_list.extend(ffdc_boot_list)
                            ffdc_boot_list = []
                            sub_boot_num = 1
                            cp_found_num = 0
                        cp_found = True
                    elif re.match(r'(^\w+\.Entry).*~(\d+)ms', line):
                        # cp = re.match(r'(^\w+\.Entry).*~(\d+)ms', line).group(1)
                        # cp_time = re.match(r'(^\w+\.Entry).*~(\d+)ms', line).group(2)
                        cp_found = True
                    elif re.match(r'(^SMM\.\w+\.Entry).*~(\d+)ms', line):
                        # cp = re.match(r'(^SMM\.\w+\.Entry).*~(\d+)ms', line).group(1)
                        # cp_time = re.match(r'(^SMM\.\w+\.Entry).*~(\d+)ms', line).group(2)
                        cp_found = True
                    elif re.match(r'(^\[.*\]\.Entry).*~(\d+)ms', line):
                        # cp = re.match(r'(^\[.*\]\.Entry).*~(\d+)ms', line).group(1)
                        # cp_time = re.match(r'(^\[.*\]\.Entry).*~(\d+)ms', line).group(2)
                        cp_found = True
                    elif re.match(r'(^BDS\.\w+\.Entry).*~(\d+)ms', line):
                        # cp = re.match(r'(^BDS\.\w+).*~(\d+)ms', line).group(1)
                        # cp_time = re.match(r'(^BDS\.\w+).*~(\d+)ms', line).group(2)
                        cp_found = True
                    elif re.match(r'Operational\: 0F\:(\d+\.\d+\.\d+\.\d+)', line):
                        #firmware_version = re.match(r'Operational\: 0F\:(\d+\.\d+\.\d+\.\d+)', line).group(1)
                        firmware_version = 'firm8'
                        #continue
                    elif re.match(r'Presented BIOS verson', line):
                        # match line like "LEPT: [GetBiosVersion] Presented BIOS verson: 1.10 IVE114P"
                        # firmware_version = line.split(' ')[-2]
                        firmware_version = 'firm9'
                        # continue
                    elif re.match(r'UcmNotifyBdsStart:AC cycle or BMC reset happened:mUcmFlags', line):
                        boot_type = "AC"
                        # continue
                    elif re.match(r'Resetting the platform \(06\)', line):
                        reset_type = "Warm"
                        sub_boot_num += 1
                    elif re.match(r'PowerButtonCallback Entry', line) or re.match(
                            r'Resetting the platform \(0E\)', line):
                        # treat the cold reboot as new boot
                        reset_type = "Cold"
                        boot_start = False
                        TOTAL_BOOT_NUM += 1
                        boot_num += 1
                        firmware_list.append(firmware_version)
                        # handle boot type
                        for m in ffdc_boot_list:
                            m['Intact'] = "N"
                            m['Boot_ID'] = boot_num
                            if boot_type == 'AC' and m['Sub_Boot_ID'] == 1:
                                m['Boot_Type'] = boot_type

                        total_ffdc_boot_list.extend(ffdc_boot_list)
                        ffdc_boot_list = []
                        sub_boot_num = 0
                        cp_found_num = 0
                        # continue
                    elif re.match(r'LenovoBootModeData.SystemBootMode \d', line):
                        # LenovoBootModeData.SystemBootMode 0
                        boot_mode = line.split(' ')[-1]
                    else:
                        pass
                        # continue

                    if cp_found:
                        cp_found = False
                        cp_found_num += 1
                        one_boot_info['Boot_Type'] = reset_type if reset_type else boot_type
                        one_boot_info['Sub_Boot_ID'] = sub_boot_num
                        # one_boot_info['Boot_ID'] = TOTAL_BOOT_NUM

                    one_boot_info['Log'] = line
                    ffdc_boot_list.append(copy.copy(one_boot_info))

                else:
                    if ("PEIM.InstallPlatformKey.Entry" in line) or \
                            ("PEIM.LenovoCryptoPpi.Entry" in line) or \
                            ("PEIM.PcdPeim.Entry" in line):
                        boot_start = True
                        # TOTAL_BOOT_NUM += 1
                        # boot_num += 1
                        sub_boot_num += 1

                        one_boot_info['Boot_Type'] = reset_type if reset_type else boot_type
                        one_boot_info['Sub_Boot_ID'] = sub_boot_num
                        # one_boot_info['Boot_ID'] = TOTAL_BOOT_NUM

                        one_boot_info['Log'] = line
                        ffdc_boot_list.append(copy.copy(one_boot_info))

        # update last firmware version
        if len(firmware_list) == boot_num and len(firmware_list):
            firmware_list[-1] = last_fw_version
            
        else:
            firmware_list.append(last_fw_version)
            

        return (boot_num, cpu_num, disk_num, memory_size, pci_num, mtm, sn, boot_mode, firmware_list, total_ffdc_boot_list)
