#URL Checker fully working
#file splitter fully working
#can recurssively call function
#can read from different csv files
#parallel processing implemented

from multiprocessing import Process, Lock
import time
import os
from pathlib import Path
from string import printable

start = time.perf_counter()

#Writing to csv file
def read_write():
    i = 1
    with open('C:\\Users\\ShyamPrasadNedumaran\\Documents\\filereader\\FileFolder\\sample_new{}.csv'.format(i), "r") as a_file: #input file
        with open('samples2.csv','w') as file: #output file
            for line in a_file:
                stripped_line = line.strip()
                a = len(stripped_line)
                if a>254 or set(stripped_line).difference(printable) :
                    print(stripped_line)
                else:
                    file.write(stripped_line)
                    file.write("\n")

#Appending to csv file using multi-processing
def read_write2(j):
    with open('C:\\Users\\ShyamPrasadNedumaran\\Documents\\filereader\\FileFolder\\sample_new{}.csv'.format(j), "r") as a_file: #input file
        with open('samples2.csv','a') as file: #output file
            for line in a_file:
                # lock.acquire()
                stripped_line = line.strip()
                a = len(stripped_line)
                if a>254 or set(stripped_line).difference(printable) :
                    print(stripped_line)
                else:
                    file.write(stripped_line)
                    file.write("\n")
                    # lock.release()

#Splitting file by 1000 lines
def split_file():
    lines_per_file = 1000
    i = 1
    smallfile = None
    with open('samples1.csv', encoding="utf-8") as bigfile:
        for lineno, line in enumerate(bigfile):
            if lineno % lines_per_file == 0:
                print(line)
                if smallfile:
                    smallfile.close()
                small_filename = 'C:\\Users\\ShyamPrasadNedumaran\\Documents\\filereader\\FileFolder\\sample_new{}.csv'.format(i)
                i = i+1
                smallfile = open(small_filename, "w")
            smallfile.write(line)
    if smallfile:
        smallfile.close()

if __name__ == "__main__":
    lock = Lock()
    processes = []

    split_file()

    #Number of files in folder
    onlyfiles = next(os.walk('C:\\Users\\ShyamPrasadNedumaran\\Documents\\filereader\\FileFolder'))[2] #dir is your directory path as string
    fileno =  len(onlyfiles) 

    read_write()

    #MultiProcessing
    # for i in range(2,fileno+1):
    #     process = Process(target=read_write2, args=(i,lock)) #creating a new process
    #     processes.append(process) #appending process to a processes list

    # for process in processes:
    #     process.start()

    # for process in processes: #loop over list to join process
    #     process.join() #process will finish before moving on with the script
    for i in range(2,fileno+1): #loop to recurssively call function
        read_write2(i)

    #Delete contents of folder
    #[f.unlink() for f in Path("C:\\Users\\ShyamPrasadNedumaran\\Documents\\filereader\\FileFolder").glob("*") if f.is_file()]

    finish = time.perf_counter() #count end time of program
    print(f'Finished in {round(finish-start, 2)} second(s)')