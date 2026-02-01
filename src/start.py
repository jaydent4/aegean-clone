import argparse, logging
from nodes.client import Client
from nodes.shim import Shim
from nodes.mixer import Mixer
from nodes.exec import Exec
from nodes.verifier import Verifier
from config import config

logger = logging.getLogger(__name__)


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.DEBUG,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )

    parser = argparse.ArgumentParser()
    parser.add_argument("--name", type=str, required=True)
    parser.add_argument("--host", type=str, required=True)
    parser.add_argument("--port", type=int, required=True)
    args = parser.parse_args()

    node_type = config[args.name]['type']
    if node_type == 'client':
        node = Client(args.name, host=args.host, port=args.port, next=config[args.name]['next'])
    elif node_type == 'shim':
        node = Shim(args.name, host=args.host, port=args.port, next=config[args.name]['next'])
    elif node_type == 'mixer':
        node = Mixer(args.name, host=args.host, port=args.port, next=config[args.name]['next'])
    elif node_type == 'exec':
        node = Exec(args.name, host=args.host, port=args.port, verifiers=config[args.name]['verifiers'])
    elif node_type == 'verifier':
        node = Verifier(args.name, host=args.host, port=args.port, next=config[args.name]['next'])
    else:
        logger.error(f"Unrecognized node type: {node_type}")
    
    node.start()