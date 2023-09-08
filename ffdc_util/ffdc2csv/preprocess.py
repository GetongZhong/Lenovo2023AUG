import os
import csv
from curses import ascii
from ffdc_util.ffdc2csv.ffdc_new import Ffdc
from utils.util import LOGGER, FIELD_NAMES
import io
import re
from datetime import datetime


class Preprocess:
    def __init__(self, ffdc_file=None):
        self.ffdc_file = ffdc_file
        self.ffdc_file_name = None
        if ffdc_file is not None:
            # self.ffdc_file_name = os.path.splitext(os.path.basename(ffdc_file))[0]
            self.ffdc_file_name = os.path.basename(ffdc_file)
        self.ffdc = None
        self.uefi_file = None
        self.uefi_csv = None
        self.boot_num = None
        self.uncompress_temp_dir = None
        try:
            self.ffdc = Ffdc(self.ffdc_file)
            self.pre_process()
            self.uncompress_temp_dir = self.ffdc.uncompress_temp_dir
            # gl._init()
            # gl.set_value('tmp_out_index_logkey', self.ffdc.uncompress_temp_dir)
            self.separate_by_boot_2_csv()
        except Exception as e:
            LOGGER.error("Parsing ffdc file error: %s!", e)
            self.uefi_file = None
            print("\033[1;35mError!!!\033[0m", e)

    def pre_process(self):
        LOGGER.info("Pre-process ffdc uefi log file.")
        if self.ffdc is not None:
            self.uefi_file = self.ffdc.merged_file + "_pre"
            LOGGER.info("%s, the uefi_file is: %s", self.ffdc_file_name, self.uefi_file)
            outlines = []
            LOG_SHIFT_LINE = re.compile(
                r'===>> log shift at (\w{3} \w{3} *\d{1,2} \d{2}:\d{2}:\d{2} \d{4})')

            with io.open(self.ffdc.merged_file, 'r', encoding='ISO-8859-1') as _pre:
                i = 0
                for line in _pre:
                    for word in line.split():
                        if not all(ascii.isascii(c) for c in word):
                            line = line.replace(word, '').strip()
                    line = line.replace('\x00', '').strip()
                    if line.__len__() < 1:
                        continue
                    m0 = LOG_SHIFT_LINE.match(line)
                    if m0 and self.ffdc.first_shift_time == None:
                        start_time = datetime.strptime(m0.group(1), "%a %b %d %H:%M:%S %Y").strftime(
                            '%Y-%m-%d %H:%M:%S')
                        self.ffdc.first_shift_time = start_time

                    newline = line + '\n'
                    outlines.append(newline)
                    i += 1
                    if i % 100000 == 0:
                        print(i)
                        
            if outlines:
                out_fp = io.open(self.uefi_file, 'w', encoding='ISO-8859-1')
                out_fp.writelines(outlines)
                out_fp.close()
                del outlines[:]

    def separate_by_boot_2_csv(self):
        LOGGER.info("second Separate uefi by single boot.")
        if self.ffdc is not None and self.uefi_file is not None:
            self.uefi_csv = self.uefi_file + ".csv"

            with io.open(self.uefi_csv, 'w+', encoding='ISO-8859-1') as lines:
                f_csv = csv.DictWriter(lines, fieldnames=FIELD_NAMES)
                f_csv.writeheader()

                if not self.ffdc.get_has_uefi_log_flag():
                    self.ffdc.delete_temp_dir()
                

                (boot_num, cpu_num, disk_num, memory_size, pci_num, mtm, sn, boot_mode,
                 firmware_list, ffdc_boot_list, uuid) = self.ffdc.generate_boot_infos_from_uefi(self.uefi_file)
                
                # self.boot_num = boot_num

                csv_info = dict()
                csv_info['Boot_Mode'] = boot_mode
                csv_info['CPU_Num'] = cpu_num
                csv_info['Disk_Num'] = disk_num
                csv_info['Memory_Size'] = memory_size
                csv_info['PCI_Num'] = pci_num
                csv_info['Mtm'] = mtm
                csv_info['SN'] = sn
                csv_info['UUid'] = uuid
                csv_info['Filename'] = self.ffdc_file_name
                i = 0
                for l in ffdc_boot_list:
                    csv_info['Log'] = l['Log']
                    
                    csv_info['Boot_ID'] = l['Boot_ID']
                    csv_info['Sub_Boot_ID'] = l['Sub_Boot_ID']
                    csv_info['Boot_Type'] = l['Boot_Type']
                    csv_info['Intact'] = l['Intact']
                    if len(firmware_list) >= boot_num and boot_num:
                        csv_info['Firmware_Version'] = firmware_list[boot_num - 1]
                    else:
                        csv_info['Firmware_Version'] = 'Unknown'
                    
                    

                    csv_info['Phase'] = l['Phase']
                    csv_info['Is_module'] = l['Is_module']
                    csv_info['Start_time'] = l['Start_time']

                    f_csv.writerow(csv_info)
                    i += 1
                    if i % 100000 == 0:
                        print(i)
                     
    def get_last_modules(csv_file):

        df = pd.read_csv(csv_file)

        last_modules = []
        Time = []
        csv = []
        start = []
        ID = []
        for i in df["Boot_ID"].unique():
        
            select = df[df["Boot_ID"] == i].iloc[-1]
            if select['Intact']=='N' :
                last_modules.append(select['Module'])
                Time.append(select["Time"])
                start.append(select['Start_time'])
                csv_file = csv_file.split('/')[-1]
                ID.append(i)
                csv.append(csv_file)
   
        return (last_modules,Time,csv,start,ID)    
