"""Only active when research-only environment gate is explicitly true."""
import os
if os.environ.get('CLOSING_BET_KRX_ONLY_RESEARCH')=='1':
    from closebet_research.krx_only_provider import install
    install()
