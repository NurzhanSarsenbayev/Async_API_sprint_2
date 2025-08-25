import aiohttp
import pytest

@pytest.mark.asyncio
async def test_healthcheck():
    async with aiohttp.ClientSession() as session:
        async with session.get("http://fastapi:8000/") as resp:
            assert resp.status == 200