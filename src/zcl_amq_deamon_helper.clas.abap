* See https://github.com/se38/abapMQ_deamon

********************************************************************************
* The MIT License (MIT)
*
* Copyright (c) 2021 Uwe Fetzer and the abapMQ Deamons Contributors
*
* Permission is hereby granted, free of charge, to any person obtaining a copy
* of this software and associated documentation files (the "Software"), to deal
* in the Software without restriction, including without limitation the rights
* to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
* copies of the Software, and to permit persons to whom the Software is
* furnished to do so, subject to the following conditions:
*
* The above copyright notice and this permission notice shall be included in all
* copies or substantial portions of the Software.
*
* THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
* IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
* FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
* AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
* LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
* OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
* SOFTWARE.
********************************************************************************


"! <p class="shorttext synchronized" lang="en">abapMQ Deamons helper routines</p>
CLASS zcl_amq_deamon_helper DEFINITION
  PUBLIC
  FINAL
  CREATE PUBLIC .

  PUBLIC SECTION.
    METHODS get_if_implementations.
    "! <p class="shorttext synchronized" lang="en">Check whether interface is implemented in class</p>
    "! @parameter i_class_name | <p class="shorttext synchronized" lang="en">Class name</p>
    "! @parameter i_interface_name | <p class="shorttext synchronized" lang="en">Interface name</p>
    "! @parameter r_result | <p class="shorttext synchronized" lang="en">Is implemented</p>
    METHODS is_if_implemented
      IMPORTING i_class_name     TYPE seoclsname
                i_interface_name TYPE seoclsname DEFAULT 'ZIF_AMQ_DEAMON'
      RETURNING VALUE(r_result)  TYPE abap_bool.

  PROTECTED SECTION.
  PRIVATE SECTION.
ENDCLASS.



CLASS zcl_amq_deamon_helper IMPLEMENTATION.
  METHOD get_if_implementations.
    "read table vseoimplem
  ENDMETHOD.

  METHOD is_if_implemented.

    SELECT SINGLE FROM vseoimplem
      FIELDS @abap_true
      WHERE clsname = @i_class_name
      AND   refclsname = @i_interface_name
      INTO @r_result.

  ENDMETHOD.

ENDCLASS.
