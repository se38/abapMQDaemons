INTERFACE zif_amq_deamon
  PUBLIC .
  METHODS on_receive
    IMPORTING i_topic   TYPE string
              i_message TYPE string.

ENDINTERFACE.
