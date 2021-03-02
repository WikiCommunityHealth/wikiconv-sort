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

def comparatorString(obj: Mapping) -> str:
    idSegments = obj['id'].split('.')

    par0 = obj['pageId'].zfill(9)
    par1 = obj['timestamp']
    par2 = idSegments[1].zfill(8)
    par3 = idSegments[2].zfill(8)
    
    return f"{par0} {par1} {par2} {par3}"

def sortFiles(
        inputFiles: Iterable[Path],
        outputPath: Path,
        bucketSize: int,
        compression: str,
    ) -> None:

    print(datetime.now().strftime("%H:%M:%S"))

    # outputFilesNames = [str(outputPath / (f"bucket-{str(i).zfill(4)}.json")) for i in range(math.ceil(nrOfPages / bucketSize))]
    # outputFiles = [file_utils.output_writer(path=filename, compression=compression) for filename in outputFilesNames]
    outputFilesNames = []
    outputFiles = []

    # Split dump
    for inputFile in inputFiles:
        utils.log(f"Analyzing {inputFile}...")

        nobjs = 0
        dump = file_utils.open_jsonobjects_file(str(inputFile))

        #process line
        for obj in dump:
            #obj = types.cast_json(raw_obj)
            #obj["timestamp"] = obj["timestamp"].isoformat()

            bucketNumber = math.floor(int(obj['pageId']) / bucketSize)

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
        utils.log(f"Done Analyzing {inputFile}.\n")
        print(datetime.now().strftime("%H:%M:%S"))


    # Closing output files
    for f in outputFiles:
        f.close()


    # Sort files
    with concurrent.futures.ThreadPoolExecutor(max_workers=4) as executor:
        # executor.map(sort, outputFilesNames)
        executor.map(lambda f: sort(f, compression), outputFilesNames)
        #executor.submit()

    # for filename in outputFilesNames:
    #     sort(filename, compression)

    print(datetime.now().strftime("%H:%M:%S"))


def sort(filename: str, compression: str):
    filename += '.' + compression

    utils.log(f"Sorting {filename}")
    if compression is None:
        os.system(f"sort {filename} -o {filename.replace('bucket', 'sorted-bucket')}")
    else:
        compressCat = EXTENSIONS.get(compression, ['cat'])
        compressor = 'gzip'
        #os.system(f"{' '.join(compressCat)} {filename} | sort -o {filename.replace('bucket', 'sorted-bucket')}")
        os.system(f"{' '.join(compressCat)} {filename} | sort | {compressor} > {filename.replace('bucket', 'sorted-bucket')}")
    utils.log(f"Done sorting {filename}\n")