import httpx
from config import settings

class IdeascaleApi():
    """ This class contains all th methods to interact with the Ideascale API"""
    def __init__(self):
        """Initialize the class setting the default headers"""
        self.headers = {'api_token': f"{settings.is_api_token}"}
        self.limits = httpx.Limits(max_keepalive_connections=5, max_connections=10)


    async def get_data_from_api(
        self,
        base_url: str,
        endpoint: str,
        params=None
    ):
        """Standard GET call to the API that returns list of models"""
        transport = httpx.AsyncHTTPTransport(retries=15)
        async with httpx.AsyncClient(
            headers=self.headers, limits=self.limits, transport=transport
        ) as client:
            result = await client.get(
                f"{base_url}{endpoint}",
                params=params,
                timeout=None
            )
            if (result.status_code != 200):
                print(result.status_code)
                print(f"{base_url}{endpoint}")
            assert result.status_code == 200
            return result.json()


    async def get_campaign_by_id(self, _id: int):
        """Get a single FullCampaign from API querying by id.
        This call will return the full response from IdeaScale.
        """
        return await self.get_data_from_api(
            settings.is_base_api_url,
            f"/v1/campaign/{_id}",
        )

    async def get_proposals_by_campaign_id(self, campaign_id: int):
        """Get the list of Proposals from API querying by Challenge"""
        page = 0
        page_size = 50
        response_size = 50
        proposals = []
        while (response_size == page_size):
            chunk_proposals = await self.get_data_from_api(
                settings.is_base_api_url,
                f"/v1/campaigns/{campaign_id}/ideas/{page}/{page_size}",
            )
            proposals.extend(chunk_proposals)
            page = page + 1
            response_size = len(chunk_proposals)
        return proposals

    '''
    async def get_proposals_by_campaign_id(self, campaign_id: int):
        """Get a single FullCampaign from API querying by id.
        This call will return the full response from IdeaScale.
        """
        return await self.get_data_from_api(
            settings.is_base_api_url,
            f"/v1/campaigns/{campaign_id}/ideas",
        )
    '''
    async def get_campaign_by_group(self, _id: int):
        return await self.get_data_from_api(
            settings.is_base_api_url,
            f"/v1/campaigns/groups/{_id}",
        )

ideascaleApi = IdeascaleApi()
