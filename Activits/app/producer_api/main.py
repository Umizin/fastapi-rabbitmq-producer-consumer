from fastapi import FastAPI, HTTPException, status
from pydantic import BaseModel
from typing import Optional
import json

from .pika_producer import publish_message 

app = FastAPI(title="API with Messaging", version="1.0.0")

# Esquema para a mensagem
class MessagePayload(BaseModel):
    name: str
    text: str
    priority: Optional[int] = 1 

@app.get("/", tags=["health"])
async def root():
    return {"status": "ok", "service": "producer_api"}

@app.post("/enviar", status_code=status.HTTP_202_ACCEPTED)
async def send_message(payload: MessagePayload):
    #Recebe um JSON e envia a mensagem para a fila do RabbitMQ.
    message_dict = payload.model_dump()
    
    success = publish_message(message_dict)
    
    if success:
        return {"status": "Message sent", "data": message_dict}
    else:
        # Retorna um erro se o RabbitMQ não estiver disponível
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Failed to connect to message broker (RabbitMQ)"
        )