import os
import io

import pandas as pd
from utils.util import LOGGER
from ffdc_util.ffdc2csv.ffdc2csv_new import write_boot_time
from ffdc_util.ffdc2csv.preprocess import Preprocess 

class Profiler:
    def __init__(self,filepath):
        self.filepath = filepath
        directory = os.path.dirname(filepath)
        try:
            self.run_profiler(directory)
        except Exception as e:
            LOGGER.error("Profiler error: %s!", e)
       
    def get_filepath(self):
        return self.filepath
    
    def save_dataframe_to_file(self, dataframe, file_path):
        dataframe_string = dataframe.to_string(index=False)
        with open(file_path, "w") as file:
            file.write(dataframe_string)

    def run_profiler(self,folder_path):
        tzz_files = [file for file in os.listdir(folder_path) if file.endswith(".tzz")]
        df_final = pd.DataFrame()
    
        for tzz in tzz_files:
            df_final = pd.DataFrame()
            ffdc = os.path.join(folder_path, tzz)
            result =  folder_path
            self.profiler(ffdc, result)
            log = '.'.join([tzz.split('.')[0],'log'])
            log_path = os.path.join(folder_path, log)
            csv = '.'.join([tzz.split('.')[0]+'_perf','csv'])
            csv_path = os.path.join(folder_path, csv)
            last_modules, Time, csv,start,ID, Intact = self.get_last_modules(csv_path)
            df = {"last_modules": last_modules, "Boot ID": ID, 'Intact': Intact,"Time": Time, 'Start Time': start, "csv": csv}
            df = pd.DataFrame(df)
            df_final = pd.concat([df_final, df])
            df_final.reset_index(drop=True)
            df_final['Boot ID'] = df_final['Boot ID'].astype(int)
            df_final['Time'] = df_final['Time'].astype(int)
            pattern1 = df_final['last_modules']
            pattern2 = df_final['Time'].astype(str)

            content = []
            for i in range(len(df_final)):
               
                output = self.get_logs_between(log_path,pattern1.iloc[i],pattern2.iloc[i])
                content.append(output)
        
            df_final['content']=content
            if os.path.exists(log_path):
                os.remove(log_path)
            else:
                print(log_path, "doesn't exists")
            if os.path.exists(csv_path):
                os.remove(csv_path)
            else:
                print(csv_path, "doesn't exists")    
            result_file = csv = '.'.join([tzz.split('.')[0],'csv'])
            result_path = os.path.join(folder_path, result_file)


            self.save_dataframe_to_file(df_final,result_path)

            
     
      
    def profiler(self,ffdc_path: str, result_dir: str):
        if not os.path.exists(ffdc_path):
            LOGGER.error('The FFDC file ' + ffdc_path + ' does not existed.')
            return dict()
        ffdc_dir = os.path.dirname(ffdc_path)
        if not os.path.exists(result_dir):
            os.makedirs(result_dir)
        preprocess = Preprocess(ffdc_path)
        if preprocess.uefi_csv is None:
            LOGGER.error("The ffdc file %s is invalid.", ffdc_path)
            return dict()
        boot_perf_file = os.path.splitext(os.path.basename(ffdc_path))[0] + "_perf.csv"
        boot_perf_file = os.path.join(ffdc_dir, boot_perf_file)
        write_boot_time(preprocess.uefi_csv, boot_perf_file, result_dir)
    
        preprocess.ffdc.delete_temp_dir()
        pass

    
    def get_logs_between(self,file_path, pattern1, pattern2):
            s = set()
            with io.open(file_path, 'r', encoding='ISO-8859-1') as file:
                lines = file.read().split('\n')
                start_index = lines.index(next(line for line in lines if pattern1 in line and pattern2 in line))

                end_index = None
                for k in range(start_index + 1, len(lines)):
                    if 'Boot Start:' in lines[k]:
                        end_index = k
                        break
                    elif 'UEFI BOOT START:' in lines[k]:
                        end_index = k
                        break
                    elif 'PeiLenovoCmosMngr.Entry' in lines[k]:
                        end_index = k
                        break

                if end_index is None:
                        end_index = len(lines)

                filtered_lines = lines[start_index:end_index]

                output = '\n'.join(filtered_lines)       
            return output  

    def get_last_modules(self, csv_file):

        df = pd.read_csv(csv_file)

        last_modules = []
        Time = []
        csv = []
        start = []
        ID = []
        Intact = []
        for i in df["Boot_ID"].unique():
        
            select = df[df["Boot_ID"] == i].iloc[-1]
        
            last_modules.append(select['Module'])
            Time.append(select["Time"])
            start.append(select['Start_time'])
            csv_file = csv_file.split('/')[-1]
            ID.append(i)
            Intact.append(select["Intact"])
            csv.append(csv_file)
   
        return (last_modules,Time,csv,start,ID, Intact)








