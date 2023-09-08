import os
import csv
from curses import ascii
from ffdc_util.ffdc2csv.ffdc import Ffdc
from utils.util import LOGGER, FIELD_NAMES
import io
import re
from datetime import datetime
class Preprocess2:

    def test(self):
        self.ffdc.get_uefi_all_module_name()

        LOGGER.info("Separate uefi by single boot.")
        if self.ffdc is not None and self.uefi_file is not None:
            self.uefi_csv = self.uefi_file + ".csv"

            with io.open(self.uefi_csv, 'w+', encoding='ISO-8859-1') as lines:
                f_csv = csv.DictWriter(lines, fieldnames=FIELD_NAMES)
                f_csv.writeheader()

                if not self.ffdc.get_has_uefi_log_flag():
                    self.ffdc.delete_temp_dir()

                s = self.ffdc.get_uefi_all_module_name(self.uefi_file)
                csv_info = dict()
                csv_info['test'] = s
                f_csv.writerow(csv_info)
