import os
import json
import math
from datetime import datetime
import concurrent.futures

from pathlib import Path
from typing import Iterable, Mapping

from . import file_utils
from . import utils

EXTENSIONS = {
    'gz': ["zcat"],
    'bz2': ["bzcat"],
    '7z': ["7z", "e", "-so"],
    'lzma': ["lzcat"]
}
NPRINTREVISION = 10000

def getBucketNumberByUsername(obj: Mapping, bucketSize: int, userField: str = 'user') -> int:
    if userField not in obj:
        return 0
    
    user = obj[userField]
    if user == "root" or user == "unknown":
        return -1

    if user is None:
        print(obj)

    if 'ip' in user:
        return 1
    elif 'id' in user:
        return math.floor(int(obj[userField]['id']) / bucketSize) + 2
    else:
        return 0

def comparatorStringByUsername(obj: Mapping, userField: str = 'user') -> str:
    idSegments = obj['id'].split('.')
    par0 = "000000000"

    if userField in obj:
        user = obj[userField]
        if 'ip' in user:
            par0 = "".join([x.zfill(3) for x in obj[userField]['ip'].split(".")])
        elif 'id' in user:
            par0 = obj[userField]['id'].zfill(12)

    par1 = obj['timestamp']
    par2 = idSegments[1].zfill(8)
    par3 = idSegments[2].zfill(8)
    
    return f"{par0} {par1} {par2} {par3}"

def getBucketNumberByPage(obj: Mapping, bucketSize: int) -> int:
    return math.floor(int(obj['pageId']) / bucketSize)

def comparatorStringByPage(obj: Mapping) -> str:
    idSegments = obj['id'].split('.')

    par0 = obj['pageId'].zfill(9)
    par1 = obj['timestamp']
    par2 = idSegments[1].zfill(8)
    par3 = idSegments[2].zfill(8)
    
    return f"{par0} {par1} {par2} {par3}"

COMPARATORS = {
    'page': comparatorStringByPage,
    'user': comparatorStringByUsername,
    'replyToUser': lambda obj: comparatorStringByUsername(obj, 'replyToUser')
}

BUCKET_NUMBER = {
    'page': getBucketNumberByPage,
    'user': getBucketNumberByUsername,
    'replyToUser': lambda obj, bucketSize: getBucketNumberByUsername(obj, bucketSize, 'replyToUser')
}

def sortFiles(
        inputFiles: Iterable[Path],
        outputPath: Path,
        bucketSize: int,
        compression: str,
        sortBy: str
    ) -> None:

    printTimestamp(outputPath, "Starting")

    # outputFilesNames = [str(outputPath / (f"bucket-{str(i).zfill(4)}.json")) for i in range(math.ceil(nrOfPages / bucketSize))]
    # outputFiles = [file_utils.output_writer(path=filename, compression=compression) for filename in outputFilesNames]
    outputFilesNames = []
    outputFiles = []

    getBucketNumber = BUCKET_NUMBER[sortBy]
    comparatorString = COMPARATORS[sortBy]


    # Split dump
    for inputFile in inputFiles:
        utils.log(f"Analyzing {inputFile}...")

        nobjs = 0
        dump = file_utils.open_jsonobjects_file(str(inputFile))

        #process line
        for obj in dump:
            #obj = types.cast_json(raw_obj)
            #obj["timestamp"] = obj["timestamp"].isoformat()

            bucketNumber = getBucketNumber(obj, bucketSize)
            if bucketNumber > 0:
                if bucketNumber >= len(outputFiles):
                    newFilenames = [str(outputPath / (f"bucket-{str(i).zfill(4)}.json")) for i in range(len(outputFiles), bucketNumber + 1)]
                    newOutputFiles = [file_utils.output_writer(path=filename, compression=compression) for filename in newFilenames]
                    outputFilesNames.extend(newFilenames)
                    outputFiles.extend(newOutputFiles)

                outputFiles[bucketNumber].write(f"{comparatorString(obj)}\t{json.dumps(obj)}\n")

            if (nobjs-1) % NPRINTREVISION == 0:
                utils.dot()
            nobjs += 1

        dump.close()
        printTimestamp(outputPath, f"Done Analyzing {inputFile}.")


    # Closing output files
    for f in outputFiles:
        f.close()

    # Sort files
    with concurrent.futures.ThreadPoolExecutor(max_workers=8) as executor:
        # executor.map(sort, outputFilesNames)
        executor.map(lambda f: sort(f, compression, outputPath), outputFilesNames)
        #executor.submit()

    # for filename in outputFilesNames:
    #     sort(filename, compression)

    printTimestamp(outputPath, f"All done!")


def sort(filename: str, compression: str, outputPath: str):

    utils.log(f"Sorting {filename}")
    printTimestamp(outputPath, f"Sorting {filename}.")
    if compression is None:
        os.system(f"sort {filename} -o {filename.replace('bucket', 'sorted-bucket')}")
    else:
        filename += '.' + compression
        compressCat = EXTENSIONS.get(compression, ['cat'])
        compressor = 'gzip'
        #os.system(f"{' '.join(compressCat)} {filename} | sort -o {filename.replace('bucket', 'sorted-bucket')}")
        os.system(f"{' '.join(compressCat)} {filename} | sort | {compressor} > {filename.replace('bucket', 'sorted-bucket')}")
    printTimestamp(outputPath, f"Done sorting {filename}.")


def printTimestamp(outputPath: Path, description: str) -> None:
    with open(str(outputPath / "times.txt"), "a") as f:
        line = f'{datetime.now().strftime("%H:%M:%S")}: {description}\n'
        f.write(line)
        print(line)
