from dataclasses import dataclass
import json

@dataclass
class ChunkMetadata:
    camera_id: str
    chunk_url: str
    timestamp: str
    duration_seconds: int

    def to_json(self):
        return json.dumps(self.__dict__) 