import os
import time
import json
from lxml import etree
import requests

import asyncio

BASEURL = "https://archief.amsterdam"
DOWNLOADURL = "https://archief.amsterdam/api/download_info/0/"
PREPAREURL = "https://archief.amsterdam/api/queue_download/0/"
HEADERS = {
    'User-Agent':
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_9_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/35.0.1916.47 Safari/537.36'
}


def makeFilenames(start, end):
    """Build a list of file names based on a shared prefix and a start and end scan. 
    
    Args:
        start (str): First scan of the inventory
        end (str): Last scan of the inventory
    
    Returns:
        list: List of filenames that are to be downloaded.
    """

    prefix = os.path.commonprefix([start, end])

    n_start = int(start.split(prefix)[1])
    n_end = int(end.split(prefix)[1])

    zpadlength = len(str(n_end))

    filenames = []
    for i in range(n_start, n_end + 1):
        filenames.append(prefix + str(i).zfill(zpadlength))

    return filenames


async def downloadScan(filename, destination='downloads/', skipIfExist=True):
    """Download a particular scan from the SAA.

    This function requests the high resolution scan from the Stadsarchief
    Amsterdam. If it is not prepared yet, it first requests a download. 
    
    Args:
        filename (str): Name of the scan as noted in the SAA interface (e.g. KLAC00161000001)
        destination (str, optional): Destination path (folder). Defaults to 'downloads/'.
    """
    if skipIfExist:
        if os.path.exists(os.path.join(destination, filename + '.pdf')):
            print('Already downloaded:', filename)
            return

    print('Downloading', filename)
    os.makedirs(destination, exist_ok=True)

    url = DOWNLOADURL + filename + ".xml"
    r = requests.get(url, headers=HEADERS)

    data = r.text
    if 'unavailable' in data:
        url = PREPAREURL + filename + '.xml'
        requests.get(url)
        print(filename,
              'Unavailable now, preparing and trying again in 10 seconds')
        await asyncio.sleep(10)

        await downloadScan(filename, destination)  # recursive
    elif 'invalid item' in data:
        print('Cannot resolve:', filename)
        return
    else:
        tree = etree.fromstring(r.content)
        download_element = tree.find("download[@label='highres']")
        url = BASEURL + download_element.find('part').attrib['url']

        r = requests.get(url, headers=HEADERS)
        with open(os.path.join(destination, filename + '.pdf'),
                  'wb') as pdffile:
            pdffile.write(r.content)


async def fetchScans(startscan, endscan, destination='downloads/'):
    """Wrapper function to download a bunch of scans based on a
    first and last scan name taken from the SAA index browser.
    
    Args:
        startscan (str): First scan of the inventory
        endscan (str): Last scan of the inventory
        destination (str, optional): Destination path (folder). Defaults to 'downloads/'.

    >>> fetchScans("KLAC00161000001", "KLAC00161000001")
    """

    filenames = makeFilenames(startscan, endscan)

    # for n, f in enumerate(filenames, 1):
    #     print(f"Downloading {n}/{len(filenames)} {f}")
    #     downloadScan(f, destination)

    await asyncio.gather(*[downloadScan(f, destination) for f in filenames])


async def fetchScansFromFile(filepath):
    """Download scans from an inventory txt file. 

    From a txt file in which every line represents a badly formed array,
    find the first and the last mentioned scan. Every line is a section
    from the inventory. The scans are grouped per section. 

    Example:
        https://archief.amsterdam/view/archive/inv/data76-81-199.txt
    
    Args:
        filepath (str): Filepath to a txt file taken from the SAA index website
    """
    with open(filepath, encoding='unicode_escape') as infile:
        lines = infile.readlines()

    all_scans = []

    for line in lines:
        items = line.split(",")

        index = items[1].replace("'", '')

        for item in items:
            if item.endswith("001'"):  # mind the accent
                prefix = item[:-3]
                break
        for item in items:
            if item.startswith(prefix):
                item = item.replace("'", '')
                all_scans.append((index, item))

    # for n, (i, scan) in enumerate(all_scans, 1):
    #     print(f"Downloading {n}/{len(all_scans)} {i}: {scan}")
    #     downloadScan(scan, destination=i + '/')

    await asyncio.gather(
        *[downloadScan(scan, destination=i + '/') for i, scan in all_scans])


if __name__ == "__main__":

    # To run from known start and end scan name:
    asyncio.run(
        fetchScans("KLAC01462000001", "KLAC01462000079", destination='30/'))

    # To run from a file that has several inventories from an index:
    # asyncio.run(fetchScansFromFile('30398.txt'))
