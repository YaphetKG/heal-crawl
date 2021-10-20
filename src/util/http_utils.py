import httpx
from urllib import parse
from pathlib import  Path
import tqdm

async def download_file(file_url, data_path='data/'):
    file = parse.unquote(file_url.split("/")[-1])
    output_path = Path(data_path) / Path(file)
    with open(output_path, mode='wb') as download_file:
        client = httpx.AsyncClient()
        req = client.build_request('GET', url=file_url)
        response = await client.send(req, stream=True)
        total = int(response.headers["Content-Length"])
        with tqdm(total=total, unit_scale=True, unit_divisor=1024, unit="B", desc=file_url) as progress:
            num_bytes_downloaded = response.num_bytes_downloaded
            async for chunk in response.aiter_bytes():
                download_file.write(chunk)
                progress.update(response.num_bytes_downloaded - num_bytes_downloaded)
                num_bytes_downloaded = response.num_bytes_downloaded
        await client.aclose()
    return str(output_path.absolute())


async def get_url(url):
    async with httpx.AsyncClient() as client:
        return await client.get(url)
