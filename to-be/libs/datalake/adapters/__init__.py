"""공급 시스템 adapter 패키지."""

from datalake.adapters.base import SupplyAdapter
from datalake.adapters.mail import MailAdapter

__all__ = ["MailAdapter", "SupplyAdapter"]
