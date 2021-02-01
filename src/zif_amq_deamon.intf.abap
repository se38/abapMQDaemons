INTERFACE zif_amq_deamon
  PUBLIC .
  METHODS on_receive
    IMPORTING is_message TYPE zif_mqtt_packet=>ty_message.

ENDINTERFACE.
