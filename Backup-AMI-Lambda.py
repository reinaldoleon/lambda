#SE DEBE AGREGAR UN INPUT A LA RULE SCHEDULE DE CLOUDWATCH CON ESTAS CARACTERISTICAS:
#Constant: { "retention":{"daily":"7","weekly":"7","monthly":"44"},"region":"eu-west-1"}

#LA FUNCION LAMBDA REALIZA AMIs DE LAS INSTANCIAS TAGEADAS

#Laambda Python 2.7 -  Controlador: lambda_function.lambda_handler y debe tener 5min de ejecución.

# -*- coding: utf-8 -*-
import boto3
import sys, os, traceback, argparse, json, calendar
import time, datetime
import re
import logging
from datetime import timedelta

def lambda_handler(event,context):
    #event constante: { "retention":{"daily":"7","weekly":"7","monthly":"44"},"region":"eu-west-1"}
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    returnCode=0
    '''
    Bloque de comentario para explicar el tema de las retenciones:
    Retention puede ser un array o un string (duck programming)
    Si es string es el modelo "legacy" en el que todas las AMIs tienen un mismo periodo de retencion
    Si es arary es el modelo "nuevo" en el que hay retenciones separadas para daily weekly y monthly
    '''

    logger.info(event)

    # conseguimos la retencion
    retention = _get_retention_value(event['retention'])
    logger.info("el valor de la retencion es: {0}".format(retention))

    region = event['region']


    try:
        # Creacion de las AMI
        ec2 = boto3.resource('ec2',region_name=region)
        logger.info("Getting list of EC2 to snapshot")
        filtro = [{'Name':'tag:Snapshot', 'Values':['1']}]
        instancias = ec2.instances.filter(Filters=filtro)
        for instancia in instancias:
            nombre = "" + datetime.datetime.today().isoformat().replace(":",".") + " backup " + instancia.id
            logger.info("Haciendo snapshot de " + instancia.id)
            obsodate = datetime.date.today() + datetime.timedelta(int(retention))
            logger.debug("  El snapshot expirara en " + obsodate.isoformat())
            imagen = instancia.create_image(Name=nombre,
                NoReboot=True,
                Description=nombre)
            logger.info("  La imagen creada es " + imagen.id)
            tags = [ {
                'Key':'ExpirationDate',
                'Value':obsodate.isoformat()
            },
            {
                'Key':'Managed',
                'Value':'1'
            },
            {
                'Key':'ip',
                'Value' : instancia.private_ip_address
            }
            ]
            for instancia_tag in instancia.tags:
                if instancia_tag['Key'] != 'Snapshot' and instancia_tag['Key'] != 'Name' and "aws" not in instancia_tag['Key']: 
                    tags.append(instancia_tag)
                elif instancia_tag['Key'] == 'Name':
                    tag_instancia_nombre = { 'Key':'InstanceName','Value':instancia_tag['Value'] }
                    tags.append(tag_instancia_nombre)
                elif "aws" in instancia_tag['Key']:
                    # do nothing
                    do_nothing=True

            imagen.create_tags(Tags=tags)

        # Deteccion y borrado de AMIs expiradas
        filtroAmi = [{'Name':'tag:Managed', 'Values':['1']}]
        amis = ec2.images.filter(Filters=filtroAmi)
        for imagen in amis:
            logger.debug("Analizando imagen: " + imagen.image_id)
            deldate=u""
            for etiqueta in imagen.tags:
                if etiqueta[u'Key'] == 'ExpirationDate':
                    deldate = etiqueta[u'Value']
            if deldate!="":
                dateOld=datetime.datetime.strptime(deldate,"%Y-%m-%d")
                if (datetime.datetime.today()>dateOld):
                    logger.info("  Se borrara la imagen " + imagen.image_id)
                    imagen.deregister()
        # Cleanup de snapshots huerfanos
        cleanOrphanSnapshots(ec2)

    except Exception, e:
        logger.error('UNHANDLED EXCEPTION')
        logger.error(str(e))
        traceback.print_exc()
        returnCode=1

    return returnCode

def _get_retention_index():
    """
    devuelve el indice (daily o monthly) en funcion del dia de la semana o del mes:
    - viernes: monthly
    - resto: daily
    """
    retentionIndex = None
    now = datetime.datetime.now()
    
    retentionIndex = 'weekly'  #retencion monthly = 44 (constante del event)
    
    return retentionIndex

def _get_retention_value(retention):
    """
    comprueba si la variable retention es string o array y la pasa a string/array de pyhton (lo que viene del evento es un json)
    lo que hacemos tambien es uniformizar que siempre sea un array con valores para weekly, monthly y daily
    """
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    returnRetention = None
    passedRetention = retention

    if isinstance(passedRetention,int):
        returnRetention = passedRetention
    else:
        if ("daily" in passedRetention) and ("weekly" in passedRetention) and ("monthly" in passedRetention):
            #json_object = json.loads(retention)
            #returnRetention = json_object[_get_retention_index()]
            returnRetention = passedRetention[_get_retention_index()]
        else:
            # la variable no define monthly ni daily ni weekly retention ni es integer
            # miramos si es convertible a integer
            try:
                if (isinstance(int(passedRetention),int)):
                    returnRetention = passedRetention
            except Exception, e:
                #territorio desconcido, default a 30 y log error
                logger.error("Could not determine the retention period, please check event setup")
                returnRetention = "30"

    return returnRetention

def cleanOrphanSnapshots(ec2):
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)
    # conseguir lista de todas las AMIs, luego lo parseamos en un array porque es menos intensivo a nivel de api
    amis = ec2.images.filter(Owners=['self'])
    ami_ids = []
    for ami in amis:
        ami_ids.append(ami.id)

    # conseguir lista de todos los snapshots, luego la parseamos en un array porque es menos intensivo a nivel de api
    snapshotslist = ec2.snapshots.filter(OwnerIds=['self'])
    snapshots = []
    for snap in snapshotslist:
        snapshots.append({'id': snap.id, 'description': snap.description})

    # esta regex nos permite saber si un snap es de una ami
    regex = '(ami-[A-z0-9]+) from vol'

    deleteable_snaps = []

    # ahora primero comprobamos si el snap es de una AMI y en tal caso si esa AMI sigue existiendo
    for snap in snapshots:
        m = re.search(regex, snap['description'])
        if m:
            if m.group(1) not in ami_ids:
                logger.info("{0} can be deleted".format(snap['id']))
                deleteable_snaps.append(snap['id'])
            else:
                logger.debug("{0} can not be deleted, is property of ami {1}".format(snap['id'], m.group(1)))

    # y ahora borramos todos los snaps borrables
    for delete_snap in deleteable_snaps:
        ec2.Snapshot(delete_snap).delete()

def cleanDuplicatedAmi(ec2):
    logger = logging.getLogger()
    logger.setLevel(logging.INFO)

    amis = ec2.images.filter(Owners=['self'])
    ami_ids = []
    for ami in amis:
        ami_ids.append({'id':ami.id,'description':ami.description,'tags':ami.tags})

    print len(ami_ids)
    print ami_ids[0]
    regex = 'backup (i-[A-z0-9]+)'

    # ahora capturamos todas las AMis que son backup
    backup_amis = []

    for ami in ami_ids:
        m = re.search(regex, str(ami['description']))
        if m:
            backup_amis.append(ami)

    backup_ami_ids = []
    regex = 'backup (i-[A-z0-9]+)'
    for backup_ami in backup_amis:
        m = re.search(regex, str(backup_ami['description']))
        expirationdate = ""
        if backup_ami['tags'] != None:
            for tag in backup_ami['tags']:
                if tag['Key'] == "ExpirationDate":
                    expirationdate = tag['Value']
        backup_ami_ids.append({"instance_id": m.group(1), "ami_id": backup_ami['id'], "expiration_date":expirationdate})

    # ahora pivotamos, usamos un set de python porque asi el elemento guardado es unico
    to_delete = set()
    for backup_ami in backup_ami_ids:
        instance_id = backup_ami['instance_id']
        ami_id = backup_ami['ami_id']
        expiration_date = backup_ami['expiration_date']
        for dup_ami in backup_ami_ids:
            dup_instance_id = dup_ami['instance_id']
            dup_ami_id = dup_ami['ami_id']
            dup_expiration_date = dup_ami['expiration_date']
            if (dup_ami_id == ami_id) and (dup_instance_id == instance_id) and (dup_expiration_date == expiration_date):
                print "Mantener ami {0} para instancia {1} y expiracion {2}".format(ami_id,instance_id,expiration_date)

            elif (dup_ami_id != ami_id) and (dup_instance_id == instance_id) and (dup_expiration_date == expiration_date):
                print "Eliminar ami {0} para instancia {1} y expiracion {2}".format(dup_ami_id,dup_instance_id,dup_expiration_date)
                to_delete.add(dup_ami_id)

    for ami_id in to_delete:
        ec2.Image(ami_id).deregister()


