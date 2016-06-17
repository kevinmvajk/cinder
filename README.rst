======
DATERA CINDER REPOSITORY
======
+---------------------+--------------------+----------------+------------------------------------+-----------------------------------+-------------------------------------------------------------------------------------------------+
| OpenStack Release   | Driver Branch Name | Driver Version | Additional capabilities introduced | Supported Datera Product Versions | Driver URL                                                                                      |
+=====================+====================+================+====================================+===================================+=================================================================================================+
| Newton (Unreleased) | newton-v2.1        | 2.1            | Multipathing, ACL                  | 1.0, 1.1                          | https://raw.githubusercontent.com/Datera/cinder/newton-v2.1/cinder/volume/drivers/datera.py     |
+---------------------+--------------------+----------------+------------------------------------+-----------------------------------+-------------------------------------------------------------------------------------------------+
| Mitaka              | mitaka-v2          | 2.0            | Baseline driver                    | 1.0, 1.1                          | https://raw.githubusercontent.com/Datera/cinder/mitaka-v2/cinder/volume/drivers/datera.py       |
+---------------------+--------------------+----------------+------------------------------------+-----------------------------------+-------------------------------------------------------------------------------------------------+
|                     | mitaka-v2.1        | 2.1            | Multipathing, ACL                  | 1.0?, 1.1                         | https://raw.githubusercontent.com/Datera/cinder/mitaka-v2.1/cinder/volume/drivers/datera.py     |
+---------------------+--------------------+----------------+------------------------------------+-----------------------------------+-------------------------------------------------------------------------------------------------+
| Liberty             | liberty-v2         | 2.0            | Baseline driver                    | 1.0, 1.1                          | https://raw.githubusercontent.com/Datera/cinder/liberty-v2/cinder/volume/drivers/datera.py      |
+---------------------+--------------------+----------------+------------------------------------+-----------------------------------+-------------------------------------------------------------------------------------------------+
|                     | liberty-v2.1       | 2.1            | Multipathing, ACL                  | 1.0?, 1.1                         | https://raw.githubusercontent.com/Datera/cinder/liberty-v2.1/cinder/volume/drivers/datera.py    |
+---------------------+--------------------+----------------+------------------------------------+-----------------------------------+-------------------------------------------------------------------------------------------------+
| Kilo                | kilo-v2            | 2.0            | Baseline driver                    | 1.0, 1.1                          | https://raw.githubusercontent.com/Datera/cinder/kilo-v2/cinder/volume/drivers/datera.py         |
+---------------------+--------------------+----------------+------------------------------------+-----------------------------------+-------------------------------------------------------------------------------------------------+
|                     | kilo-v2.1          | 2.1            | Multipathing, ACL                  | 1.0?, 1.1                         | https://raw.githubusercontent.com/Datera/cinder/kilo-v2.1/cinder/volume/drivers/datera.py       |
+---------------------+--------------------+----------------+------------------------------------+-----------------------------------+-------------------------------------------------------------------------------------------------+
| Juno                | juno-v2            | 2.0            | Baseline driver                    | 1.0, 1.1                          | https://raw.githubusercontent.com/Datera/cinder/juno-v2/cinder/volume/drivers/datera.py         |
+---------------------+--------------------+----------------+------------------------------------+-----------------------------------+-------------------------------------------------------------------------------------------------+
|                     | juno-v2.1          | 2.1            | Multipathing, ACL                  | 1.0, 1.1                          | in progress                                                                                     |
+---------------------+--------------------+----------------+------------------------------------+-----------------------------------+-------------------------------------------------------------------------------------------------+
| Icehouse            | icehouse-v2        | 2.0            | Baseline driver                    | 1.0, 1.1                          | https://raw.githubusercontent.com/Datera/cinder/icehouse-v2/cinder/volume/drivers/datera.py     |
+---------------------+--------------------+----------------+------------------------------------+-----------------------------------+-------------------------------------------------------------------------------------------------+
|                     | icehouse-v2.1      | 2.1            | Multipathing, ACL                  | 1.0, 1.1                          | in progress                                                                                     |
+---------------------+--------------------+----------------+------------------------------------+-----------------------------------+-------------------------------------------------------------------------------------------------+

=======
Configuration Options
=======

.. list-table:: Description of Datera volume driver configuration options
   :header-rows: 1
   :class: config-ref-table

   * - Configuration option = Default value
     - Description
   * - **[DEFAULT]**
     -
   * - ``datera_api_port`` = ``7717``
     - (String) Datera API port.
   * - ``datera_api_version`` = ``2``
     - (String) Datera API version.
   * - ``datera_num_replicas`` = ``1``
     - (String) Number of replicas to create of an inode.
   * - ``driver_client_cert`` = ``None``
     - (String) The path to the client certificate for verification, if the driver supports it.
   * - ``driver_client_cert_key`` = ``None``
     - (String) The path to the client certificate key for verification, if the driver supports it.
   * - ``datera_503_timeout`` = ``120``
     - (Int) Timeout for HTTP 503 retry messages
   * - ``datera_503_interval`` = ``5``
     - (Int) Interval between 503 retries
   * - ``datera_acl_allow_all`` = ``False``
     - (Bool) True to set acl 'allow_all' on volumes created
   * - ``datera_debug`` = ``False``
     - (Bool) True to set function arg and return logging
