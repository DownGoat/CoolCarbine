from domain import SessionPairResultsDto


class CoreComponent:
    async def next(self, session_pair_results: SessionPairResultsDto):
        raise NotImplementedError('You have not implemented this.')