import yagmail

def enviar_correo(request):
    remitente = "datapioneerconsulting@gmail.com"
    password = "nyfq vmez dwye cyda"
    destinatario = "gustavoadolfogonz@gmail.com" , "marianaballardini@gmail.com"
    asunto = "Proceso Completado en la Nube"
    cuerpo = "El proceso de carga de datos a Cloud Storage, procesamiento y creacion de tablas en Big Query ha sido completado exitosamente."
    
    try:
        # Crear el objeto de yagmail
        yag = yagmail.SMTP(remitente, password)

        # Enviar el correo
        yag.send(to=destinatario, subject=asunto, contents=cuerpo)

        print("Correo enviado exitosamente!")
    except Exception as e:
        print(f"Error al enviar el correo: {e}")

    return "Correo procesado", 200



