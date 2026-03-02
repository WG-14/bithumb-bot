from bithumb_bot.oms import new_client_order_id, create_order
cid = new_client_order_id("testopen")
create_order(client_order_id=cid, side="BUY", qty_req=0.001, price=1.0, status="NEW", ts_ms=1)
print("created", cid)
