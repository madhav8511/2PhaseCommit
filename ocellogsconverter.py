import json
import os
import re

def load_any_json(path):
    if not os.path.exists(path) or os.path.getsize(path) == 0:
        return []

    with open(path, "r") as f:   # no need r+
        content = f.read().strip()

    try:
        data = json.loads(content)
        if isinstance(data, list):
            return data
        return [data]
    except:
        pass

    data = []
    with open(path, "r") as f:
        for line in f:
            try:
                data.append(json.loads(line))
            except:
                pass
    return data


# ---------------------- FIXED FUNCTION ---------------------- #
def add_objects(logs, ocellogs):
    for log in logs:
        obj = {"attributes": []}
        ts = log["@timestamp"]
        message = log["message"]

        # REAL FIX: extract key:value pairs using REGEX
        # Supports arrays, strings, numbers, everything
        pairs = re.findall(r'(\w+)\s*:\s*(\[[^\]]*\]|[^,\s]+)', message)

        for key, value in pairs:
            key = key.strip()
            value = value.strip()

            # skip IDs in event attributes
            if key.lower() in ["correlationid", "timestamp"]:
                continue

            # id
            if key in ["id", "orderId"]:
                obj["id"]=str(value)
                continue

            # type detection
            if key == "eventType":
                if value in ["FinalizeOrder", "CreateOrder"]:
                    obj["type"] = "order"
                elif value == "DeductItems":
                    obj["type"] = "item"
                else:
                    obj["type"] = "user"
                continue

            # default → attribute
            attribute = {
                "name": key,
                "value": value,
                "time": ts
            }
            obj["attributes"].append(attribute)

        ocellogs[0]["objects"].append(obj)

    return ocellogs

# ------------------------------------------------------------ #
def get_qualifier(eventType):
    if eventType=="CreateOrder":
        return "order"
    elif eventType=="FinalizeOrder":
        return "status"
    elif eventType=="DeductItems":
        return "quantity"
    elif eventType=="ChargeMoney":
        return "balance"

def add_relationships(logs,ocellogs):
    for log in logs:
        message = log["message"]
        pairs = re.findall(r'(\w+)\s*:\s*(\[[^\]]*\]|[^,\s]+)', message)

        correlationid=-1
        eventType =""
        id=""
        for key, value in pairs:
            key = key.strip()
            value = value.strip()
            if key.lower()=="correlationid" or key.lower() == "correlationId":
                correlationid = value
            elif key=="eventType":
                eventType=value
            elif key in ["id", "orderId"]:
                id=value
        

        #Now iterate over all events and where eventype and correlationid matches there just put 
        for event in ocellogs[0]["events"]:
            if event["id"]==correlationid and event["type"]==eventType:
                event["relationships"].append({"id":id,"qualifier":get_qualifier(eventType)})
        
    return ocellogs
                






orderlogs     = load_any_json("/home/madhav/Desktop/2PhaseCommit/order-service/logs/database-operations.json")
inventorylogs = load_any_json("/home/madhav/Desktop/2PhaseCommit/inventory-service/logs/database-operations.json")
paymentlogs   = load_any_json("/home/madhav/Desktop/2PhaseCommit/payment-service/logs/database-operations.json")
kafkalogs     = load_any_json("/home/madhav/Desktop/2PhaseCommit/logging-service/src/main/java/com/example/logging_service/data/events.json")
ocellogs      = load_any_json("ocel.json")

ocellogs[0]["events"] = []
ocellogs[0]["objects"] = []

# EVENTS
for log in kafkalogs[0]["logs"]:
    data = {}
    if log.get("correlationid")==None:
        data["id"]=log["correlationId"]
    else:
        data["id"]=log["correlationid"]
    data["type"] = log["eventype"]
    data["time"] = log["timestamp"]
    data["attributes"] = [{"name": k, "value": v} for k, v in log["data"].items()]
    data["relationships"] = []
    ocellogs[0]["events"].append(data)

# OBJECTS
ocellogs = add_objects(orderlogs, ocellogs)
ocellogs = add_objects(inventorylogs, ocellogs)
ocellogs = add_objects(paymentlogs, ocellogs)

ocellogs = add_relationships(orderlogs,ocellogs)
ocellogs = add_relationships(inventorylogs, ocellogs)
ocellogs = add_relationships(paymentlogs, ocellogs)

print(json.dumps(ocellogs, indent=2))
