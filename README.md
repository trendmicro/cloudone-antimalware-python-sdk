# Trend Cloud One VSAPI SDK for Python

Cloud One VSAPI is a Software Development Kit (SDK) for Python, which allows Python developers to write software that makes use of Cloud One Antimalware Service API.

## Requirements

- Have an [Trend Cloud One Account](https://cloudone.trendmicro.com). [Sign up for free trial now](https://cloudone.trendmicro.com/trial) if it's not already the case!
- A [Trend Cloud One API Key](https://cloudone.trendmicro.com/docs/identity-and-account-management/c1-api-key/#new-api-key)
- A [Trend Cloud One Region](https://cloudone.trendmicro.com/docs/identity-and-account-management/c1-regions/) of choice
- Python 3.7 or newer
- A file or object to be scan



## Installation

Install the VSAPI SDK package with pip:

   ```sh
   python -m pip install cloudone-vsapi
   ```

## Documentation

Documentation for the client SDK is available on [Here](README.md) and [Read the Docs](https://cloudone.trendmicro.com/docs/).

## Run SDK

### Run with Cloud One VSAPI examples

1. Go to `/examples/` in current directory.

   ```sh
   cd examples/
   ```

2. There are two Python examples in the folder, one with regular file i/o and one with asynchronous file i/o
   
   ```
   client_aio.py
   client.py
   ``` 
3. Current Python examples support following command line arguments

   | Command Line Arguments                 | Value                    | Optional |
   | :------------------ | :----------------------- | :------- |
   | --region or -r | [Trend Cloud One Region](https://cloudone.trendmicro.com/docs/identity-and-account-management/c1-regions/), such as: us-1 | Yes, either -r or -a
   | --addr or -a   | Trend Cloud One Amaas server, such as: antimalware.us-1.cloudone.trendmicro.com:443 | Yes, either -r or -a      |
   | --api_key      | Cloud One \<API KEY\>              | No       |
   | --filename or -f |        File to be scanned            | No       |


4. Run one of the examples.
   
   The example program needs to be configured with your Cloud One account's secret key which is available in your Cloud One Dashboard. Set `API_KEY` from corresponding Cloud One Region to its value and `FILENAME` to the target file:

   ```sh
   python3 client.py -f FILENAME -r us-1 --api_key API_KEY
   ```
   or
   
   using Antimalware Service server address `-a` instead of region `-r`:
   ```sh
   python3 client.py -f FILENAME -a antimalware.us-1.cloudone.trendmicro.com:443 --api_key API_KEY
   ```
   or
   
   using asynchronous IO example program:
   ```sh
   python3 client_aio.py -f FILENAME -a antimalware.us-1.cloudone.trendmicro.com:443 --api_key API_KEY
   ```
### Code Examples
```python:
import json
import amaas.grpc

handle = amaas.grpc.init(YOUR_CLOUD_ONE_AMAAS_SERVER, YOUR_ClOUD_ONE_KEY, True)

result = amaas.grpc.scan_file(args.filename, handle)
print(result)

result_json = json.loads(result)
print("Got scan result: %d" % result_json['scanResult'])

amaas.grpc.quit(handle)

```

to use asyncio with  coroutines and tasks, 

```python:
import json
import pprint
import asyncio
import amaas.grpc.aio

async def scan_files():
    handle = amaas.grpc.aio.init(YOUR_CLOUD_ONE_AMAAS_SERVER, YOUR_ClOUD_ONE_KEY, True)

    tasks = [asyncio.create_task(amaas.grpc.aio.scan_file(file_name, handle))]

    scan_results = await asyncio.gather(*tasks)

    for scan_result in scan_results:
        pprint.pprint(json.loads(scan_result))

    await amaas.grpc.aio.quit(handle)


asyncio.run(scan_files())

```

## More Resources
- [License](https://github.com/trendmicro/cloudone-antimalware-python-sdk/blob/main/LICENSE)
- [Changelog](https://github.com/trendmicro/cloudone-antimalware-python-sdk/blob/main/CHANGELOG.md)
- [Notice](https://github.com/trendmicro/cloudone-antimalware-python-sdk/blob/main/NOTICE)
