# import json
# import os

# def load_any_json(path):
#     # Read whole file content
#     if not os.path.exists(path) or os.path.getsize(path) == 0:
#         return []

#     with open(path, "r+") as f:
#         content = f.read().strip()

#     # CASE 1 → Already a valid JSON array or object
#     try:
#         data = json.loads(content)
#         if isinstance(data, list):
#             return data
#         return [data]  # wrap single object as list
#     except:
#         pass

#     # CASE 2 → NDJSON or multiple JSON objects (one per line)
#     data = []
#     with open(path, "r+") as f:
#         for line in f:
#             line = line.strip()
#             if not line:
#                 continue
#             try:
#                 data.append(json.loads(line))
#             except:
#                 continue
#     return data

# def add_objects(logs,ocellogs):
#     for log in logs:
#         object={}
#         object["attributes"]=[]
#         l = list(log["message"].split(","))
#         ts = log["@timestamp"]

#         for i in range(len(l)):
#             rem_attributes={}
#             temp = list(l[i].split(":"))
#             print(temp)
#             temp[0]=temp[0].strip()
#             temp[1]=temp[1].strip()
#             if temp[0]=="correlationid" or temp[0]=="correlationId" or temp[0]=="timestamp":
#                 continue
#             if temp[0]=="id" or temp[0]=="orderId":
#                 object["id"]=int(temp[1])
#             elif temp[0]=="eventType":
#                 if temp[1]=="FinalizeOrder" or temp[1]=="CreateOrder":
#                     object["type"]="order"
#                 elif temp[1]=="DeductItems":
#                     object["type"]="item"
#                 else:
#                     object["type"]="user"
#             else:
#                 rem_attributes["name"]=temp[0]
#                 rem_attributes["value"]=temp[1]
#                 rem_attributes["time"]=ts
#                 object["attributes"].append(rem_attributes)
        
#         ocellogs[0]["objects"].append(object)
    
#     return ocellogs



        


# inventorylogs = load_any_json("/home/siddhesh/Documents/parent-project/inventory-service/logs/database-operations.json")
# orderlogs     = load_any_json("/home/siddhesh/Documents/parent-project/order-service/logs/database-operations.json")
# paymentlogs   = load_any_json("/home/siddhesh/Documents/parent-project/payment-service/logs/database-operations.json")
# kafkalogs     = load_any_json("/home/siddhesh/Documents/parent-project/logging-service/src/main/java/com/example/logging_service/data/events.json")
# ocellogs      = load_any_json("ocel.json")
# ocellogs[0]["events"]=[]
# ocellogs[0]["objects"]=[]


# for log in kafkalogs[0]["logs"]:
#     # print(log)
#     data={}
#     if log.get("correlationid")==None:
#         data["id"]=log["correlationId"]
#     else:
#         data["id"]=log["correlationid"]
#     data["type"]=log["eventype"]
#     data["time"]=log["timestamp"]
#     data["attributes"]=[]
#     for key,value in log["data"].items():
#         data["attributes"].append({"name":key,"value":value})
#     data["relationships"]=[]

#     ocellogs[0]["events"].append(data)

# ocellogs = add_objects(orderlogs,ocellogs)
# ocellogs = add_objects(inventorylogs,ocellogs)
# ocellogs=add_objects(paymentlogs,ocellogs)

# print(ocellogs)

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
        pairs = re.findall(r'(\w+)\s*:\s*([^,]+(?:\[[^\]]*\])?)', message)

        for key, value in pairs:
            key = key.strip()
            value = value.strip()

            # skip IDs in event attributes
            if key.lower() in ["correlationid", "timestamp"]:
                continue

            # id
            if key in ["id", "orderId"]:
                try:
                    obj["id"] = int(value)
                except:
                    obj["id"] = value
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



inventorylogs = load_any_json("/home/siddhesh/Documents/parent-project/inventory-service/logs/database-operations.json")
orderlogs     = load_any_json("/home/siddhesh/Documents/parent-project/order-service/logs/database-operations.json")
paymentlogs   = load_any_json("/home/siddhesh/Documents/parent-project/payment-service/logs/database-operations.json")
kafkalogs     = load_any_json("/home/siddhesh/Documents/parent-project/logging-service/src/main/java/com/example/logging_service/data/events.json")
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

print(json.dumps(ocellogs, indent=2))
