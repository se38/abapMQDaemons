*&---------------------------------------------------------------------*
*& Report zamq_deamon_monitor
*&---------------------------------------------------------------------*
*&
*&---------------------------------------------------------------------*
REPORT zamq_deamon_monitor.

CLASS app DEFINITION CREATE PUBLIC.

  PUBLIC SECTION.
    METHODS main.
  PROTECTED SECTION.
  PRIVATE SECTION.

ENDCLASS.

NEW app( )->main( ).

CLASS app IMPLEMENTATION.

  METHOD main.



    SELECT * FROM zamq_deamons
      INTO TABLE @DATA(deamons).

    TRY.
        cl_salv_table=>factory(
          IMPORTING
            r_salv_table   = DATA(alv)
          CHANGING
            t_table        = deamons
        ).

        alv->get_columns( )->get_column( 'GUID' )->set_technical( abap_true ).
        alv->get_columns( )->get_column( 'MANDT' )->set_technical( abap_true ).
        alv->get_columns( )->set_optimize( abap_true ).

        alv->get_display_settings( )->set_striped_pattern( abap_true ).

      CATCH cx_salv_msg
            cx_salv_not_found.
    ENDTRY.



    alv->display( ).

  ENDMETHOD.

ENDCLASS.
