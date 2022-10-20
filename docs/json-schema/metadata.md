```json
{
  "$id": "Metadata",
  "description": "Object related to the metadata of an event",
  "type": "object",
  "properties": {
    "agent": {
      "type": "string"
    },
    "origin": {
      "type": "object",
      "properties": {
        "endpoint": {
          "type": "string"
        },
        "method": {
          "type": "string",
          "enum": ["POST", "PATCH", "PUT", "DELETE"]
        }
      },
      "required": ["endpoint", "method"]
    },
    "createdAt": {
      "type": "string"
    }
  },
  "required": ["agent", "createdAt"]
}
```