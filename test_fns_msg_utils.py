import json
from unittest import TestCase
from fns_msg_utils import resize_updates

# An order book snapshot with 3 order updates
EXAMPLE_MESSAGE_JSON = """
{
  "updates": [
    {
      "orderbookUpdate": {
        "updates": [
          {
            "orderPlace": {
              "order": {
                "orderId": {
                  "subaccountId": {
                    "owner": "dydx15u3dtsf4twdxttvy7850dkex7tcf3ps2y8wcuf"
                  },
                  "clientId": 1889199344
                },
                "side": "SIDE_BUY",
                "quantums": "1487000000",
                "subticks": "6081600000",
                "goodTilBlock": 23392187,
                "timeInForce": "TIME_IN_FORCE_POST_ONLY"
              },
              "placementStatus": "ORDER_PLACEMENT_STATUS_BEST_EFFORT_OPENED"
            }
          },
          {
            "orderUpdate": {
              "orderId": {
                "subaccountId": {
                  "owner": "dydx15u3dtsf4twdxttvy7850dkex7tcf3ps2y8wcuf"
                },
                "clientId": 1889199344
              }
            }
          },
          {
            "orderPlace": {
              "order": {
                "orderId": {
                  "subaccountId": {
                    "owner": "dydx1dax7t2529z8996579zuqdatv62wpsw89lv3apc"
                  },
                  "clientId": 1669966706
                },
                "side": "SIDE_BUY",
                "quantums": "1533000000",
                "subticks": "6081600000",
                "goodTilBlock": 23392187,
                "timeInForce": "TIME_IN_FORCE_POST_ONLY"
              },
              "placementStatus": "ORDER_PLACEMENT_STATUS_BEST_EFFORT_OPENED"
            }
          }
        ],
        "snapshot": true
      },
      "blockHeight": 23392185,
      "execMode": 102
    }
  ]
}
"""

# A series of snapshots resized to have no more than 2 order updates each. The
# rest of the message frames should be equal.
EXAMPLE_RESIZED = """
[
  {
    "updates": [
      {
        "orderbookUpdate": {
          "updates": [
            {
              "orderPlace": {
                "order": {
                  "orderId": {
                    "subaccountId": {
                      "owner": "dydx15u3dtsf4twdxttvy7850dkex7tcf3ps2y8wcuf"
                    },
                    "clientId": 1889199344
                  },
                  "side": "SIDE_BUY",
                  "quantums": "1487000000",
                  "subticks": "6081600000",
                  "goodTilBlock": 23392187,
                  "timeInForce": "TIME_IN_FORCE_POST_ONLY"
                },
                "placementStatus": "ORDER_PLACEMENT_STATUS_BEST_EFFORT_OPENED"
              }
            },
            {
              "orderUpdate": {
                "orderId": {
                  "subaccountId": {
                    "owner": "dydx15u3dtsf4twdxttvy7850dkex7tcf3ps2y8wcuf"
                  },
                  "clientId": 1889199344
                }
              }
            }
          ],
          "snapshot": true
        },
        "blockHeight": 23392185,
        "execMode": 102
      }
    ]
  },
  {
    "updates": [
      {
        "orderbookUpdate": {
          "updates": [
            {
              "orderPlace": {
                "order": {
                  "orderId": {
                    "subaccountId": {
                      "owner": "dydx1dax7t2529z8996579zuqdatv62wpsw89lv3apc"
                    },
                    "clientId": 1669966706
                  },
                  "side": "SIDE_BUY",
                  "quantums": "1533000000",
                  "subticks": "6081600000",
                  "goodTilBlock": 23392187,
                  "timeInForce": "TIME_IN_FORCE_POST_ONLY"
                },
                "placementStatus": "ORDER_PLACEMENT_STATUS_BEST_EFFORT_OPENED"
              }
            }
          ],
          "snapshot": true
        },
        "blockHeight": 23392185,
        "execMode": 102
      }
    ]
  }
]
"""


class Test(TestCase):
    def test_resize_updates_matches_example(self):
        resized = resize_updates(json.loads(EXAMPLE_MESSAGE_JSON), 2)
        self.assertEqual(
            json.dumps(resized, indent=2),
            json.dumps(json.loads(EXAMPLE_RESIZED), indent=2)
        )

    def test_resize_updates_message_frames(self):
        # Test that the message frames are equal by checking that the first
        # resized message is identical to just truncating the original message
        # updates
        large_snapshot = json.loads(EXAMPLE_MESSAGE_JSON)
        resized = resize_updates(json.loads(EXAMPLE_MESSAGE_JSON), 2)
        large_snapshot['updates'][0]['orderbookUpdate']['updates'] = large_snapshot['updates'][0]['orderbookUpdate']['updates'][:2]

        self.assertEqual(
            json.dumps(resized[0], indent=2),
            json.dumps(large_snapshot, indent=2)
        )

        # Same test for the second resized message
        large_snapshot = json.loads(EXAMPLE_MESSAGE_JSON)
        large_snapshot['updates'][0]['orderbookUpdate']['updates'] = large_snapshot['updates'][0]['orderbookUpdate']['updates'][2:]
        self.assertEqual(
            json.dumps(resized[1], indent=2),
            json.dumps(large_snapshot, indent=2)
        )