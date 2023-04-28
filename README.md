# Cloud One VSAPI SDK for Python

Cloud One VSAPI is a Software Development Kit (SDK) for Python, which allows Python developers to write software that makes use of Cloud One Antimalware Service.

## Prerequisites

- Python 3.7 or newer
- [CloudOne API Key](https://cloudone.trendmicro.com/docs/identity-and-account-management/c1-api-key/)


## Installation

Install the VSAPI SDK package with pip:

   ```sh
   python -m pip install cloudone-vsapi
   ```

## Documentation

Documentation for the client SDK is available on [Here]() and [Read the Docs](https://cloudone.trendmicro.com/docs/).

## Run SDK

### Example Usage
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
   | --addr or -a   | antimalware.us-1.cloudone.trendmicro.com:443 | No       |
   | --api_key      | Cloud One \<API KEY\>              | No       |
   | --filename or -f |        File to be scanned            | No       |



4. Run one of the examples.

   ```sh
   python3 client.py -f FILENAME -a ADDR --api_key API_KEY
   ```
   or
   ```sh
   python3 client_aio.py -f FILENAME -a ADDR --api_key API_KEY
   ```
## More Resources
- [License](LICENSE)
- [Changelog](CHANGELOG.md)
- [NOTICE](NOTICE)
