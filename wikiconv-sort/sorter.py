import os
import json
import math
import datetime
import concurrent.futures

from pathlib import Path
from typing import Iterable, Mapping

from . import file_utils
from . import types
from . import utils

EXTENSIONS = {
    'gz': ["zcat"],
    'bz2': ["bzcat"],
    '7z': ["7z", "e", "-so"],
    'lzma': ["lzcat"]
}
NPRINTREVISION = 10000


def processFile(inputFilename: str, outFiles: list, bucketSize: int, outputPath: str, compression: str) -> None:
    nobjs = 0
    dump = file_utils.open_jsonobjects_file(str(inputFilename))
    for raw_obj in dump:
        obj = types.cast_json(raw_obj)
        obj["timestamp"] = obj["timestamp"].isoformat()
        bucketNumber = math.floor(obj['pageId'] / bucketSize)

        if bucketNumber >= len(outFiles):
            outputFilesNames = [str(outputPath / (f"bucket-{str(i).zfill(4)}.json")) for i in range(len(outFiles), bucketNumber + 1)]
            outputFiles = [file_utils.output_writer(path=filename, compression=compression) for filename in outputFilesNames]
            outFiles.extend(outputFiles)

        outFiles[bucketNumber].write(f"{comparatorString(obj)}\t{json.dumps(obj)}\n")

        if (nobjs-1) % NPRINTREVISION == 0:
            utils.dot()
        nobjs += 1

    dump.close()

    for f in outFiles:
        f.close()
    return outputFilesNames

def comparatorString(obj: Mapping) -> str:
    idSegments = obj['id'].split('.')

    par0 = str(obj['pageId']).zfill(9)
    par1 = str(obj['timestamp'])
    par2 = idSegments[1].zfill(8)
    par3 = idSegments[2].zfill(8)
    
    return f"{par0} {par1} {par2} {par3}"

def sortFiles(
        inputFiles: Iterable[Path],
        outputPath: Path,
        bucketSize: int,
        compression: str,
    ) -> None:

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
        for raw_obj in dump:
            obj = types.cast_json(raw_obj)
            obj["timestamp"] = obj["timestamp"].isoformat()
            bucketNumber = math.floor(obj['pageId'] / bucketSize)

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
        utils.log(f"Done Analyzing {inputFile}.")
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
    utils.log(f"Done sorting {filename}")