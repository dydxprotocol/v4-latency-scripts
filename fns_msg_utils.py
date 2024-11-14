"""
Utils for splitting gRPC stream messages into smaller batches to fall within
BigQuery's row size limit.
"""


def is_orderbook_snapshot(x):
    """
    Is one of the top level objects, x, in an 'updates' batch from the gRPC stream a
    snapshot?

    Each stream message is shaped {'updates': [update1, update2, ...]} where each
    update may be a snapshot.
    """
    return 'orderbookUpdate' in x and x['orderbookUpdate'].get('snapshot', False)


def split_orderbook_snapshot(x, n):
    """
    Split a snapshot update into a series of smaller updates, with n order updates per.
    """

    def construct_msg_frame():
        outer = {k: v if k != 'orderbookUpdate' else {} for k, v in x.items()}
        inner = {k: v if k != 'updates' else [] for k, v in x['orderbookUpdate'].items()}
        outer['orderbookUpdate'] = inner
        return outer

    snapshot_order_messages = x['orderbookUpdate']['updates']
    xs = []
    i = 0
    while i < len(snapshot_order_messages):
        msg = construct_msg_frame()
        msg['orderbookUpdate']['updates'] = snapshot_order_messages[i:i + n]
        xs.append(msg)
        i += n
    return xs


def resize_updates(x, n):
    """
    Split a message shaped {"updates": [update1, update2, ...]} into multiple messages of
    the same shape, [{"updates": [updateA, updateB, ...]}, {"updates": [...]}], where
    large book snapshot messages are split up.
    """
    def construct_msg_frame():
        return {k: v if k != 'updates' else [] for k, v in x.items()}

    msgs = []
    top_level_updates = []
    for top_level_update in x['updates']:
        # Upon encountering a snapshot...
        if is_orderbook_snapshot(top_level_update):
            # Split out any updates seen so far into their own message
            if top_level_updates:
                msg = construct_msg_frame()
                msg['updates'] = top_level_updates
                msgs.append(msg)
                top_level_updates = []

            # Split the snapshot into a series of updates and assign each to its own
            # message
            for snapshot_part in split_orderbook_snapshot(top_level_update, n):
                msg = construct_msg_frame()
                msg['updates'] = [snapshot_part]
                msgs.append(msg)
        else:
            top_level_updates.append(top_level_update)

    # Append any remaining updates
    if top_level_updates:
        msg = construct_msg_frame()
        msg['updates'] = top_level_updates
        msgs.append(msg)

    return msgs
