config = {
    'node1': {'type': 'client', 'next': ('node4', 'node5', 'node6')},
    'node2': {'type': 'client', 'next': ('node4', 'node5', 'node6')},
    'node3': {'type': 'client', 'next': ('node4', 'node5', 'node6')},
    'node4': {'type': 'shim', 'next': 'node7'},
    'node5': {'type': 'shim', 'next': 'node8'},
    'node6': {'type': 'shim', 'next': 'node9'},
    'node7': {'type': 'mixer', 'next': 'node10'},
    'node8': {'type': 'mixer', 'next': 'node11'},
    'node9': {'type': 'mixer', 'next': 'node12'},
    'node10': {'type': 'exec', 'next': 'node13'},
    'node11': {'type': 'exec', 'next': 'node14'},
    'node12': {'type': 'exec', 'next': 'node15'},
}