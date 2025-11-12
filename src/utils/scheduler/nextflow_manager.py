from dataclasses import dataclass, field
from utils.logger import get_logger

logger = get_logger(__name__)

@dataclass(slots=True)
class NextflowManager:
    
