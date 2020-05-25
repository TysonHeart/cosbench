COSBench - Cloud Object Storage Benchmark
=========================================

COSBench is a benchmarking tool to measure the performance of Cloud Object Storage services. Object storage is an
emerging technology that is different from traditional file systems (e.g., NFS) or block device systems (e.g., iSCSI).
Amazon S3 and Openstack* swift are well-known object storage solutions.

COSBench now supports OpenStack* Swift, Amazon* S3, Amplidata v2.3, 2.5 and 3.1, Scality*, Ceph, CDMI, Google* Cloud Storage, Aliyun OSS as well as custom adaptors.

** This fork of cosbench has the below enhancements over the original cosbench repo:
  - Upgrades the httpclient library used in cosbench-s3 plugin to 4.5.12.
  - Upgrades the aws s3 client (sdk) library used in cosbench-s3 plugin to 1.11.700.
  - Adds support for adding lifecycle rules to buckets via cosbench provided specifiers.
  - Adds support for adding object tags to objects (after writing them) via cosbench provided specifiers.
** 

Parameters for adding lifecycle rules and object tags 
-----------------------------------------------------
> <storage type="s3" config="accesskey=XXXXXXXXXXXXXXXXXXXX;secretkey=XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX;endpoint=http://<host:port>;path_style_access=true;lc_rules_count=c(10);lc_rules=userid|uservalueprefix|r(1,100):pci_status|pci|r(1,2)=u(1,3);object_tags=userid|uservalueprefix|u(1,100):pci_status|pci|u(1,2)"/>

The parameters lc_rules_count and lc_rules are used during the application of lifecycle configuration to a bucket.

- lc_rules_count: Number of rules in lifecycle configuration per bucket. Format: <some cosbench specifier>. Optional parameter, default value of this 'lc_rules_count' is 'c(1)'.
- lc_rules: The definition of lifecycle rules. Format: <colon separated list of tag matchers>=<expiration days specifier>
-- Here a tag matcher is of the format
  > <tag key>|<tag value prefix>|<tag value suffix specifier>

  A tag key is a static string whereas a tag value is composed of a static prefix followed by a specifier separated by a colon.

- Expiration days specifier generates an expiration value in days.

For each bucket, all specifiers in the parameter configuration are triggered for their next set of values and those are used in the lifecycle configuration.
Lifecycle rules are re-applied even if bucket already exists and not just during creation of a bucket.

The object tags are applied to an object immediately after the object is written to the object store.
The 'object_tags' parameter is used to generate object tags to apply to each object.

The value of the parameter 'object_tags' is a colon separated list of tags. A tag is a combination of tag key and tag value. A tag key is a static string whereas a tag value is composed of a static prefix followed by a specifier separated by a colon.
- object_tags=<colon separated list of tag matchers>.
-- Here a tag matcher is of the format
  > <tag key>|<tag value prefix>|<tag value suffix specifier>

For each object, all specifiers in the parameter configuration are triggered for their next set of values and those are used in object tags applied to an object.


Important Notice and Contact Information
----------------------------------------

a) COSBench is not a product, and it does not have a full-time support team. Before you use this tool, please understand 
the need to invest enough effort to learn how to use it effectively and to address possible bugs.

b) To help COSBench develop further, please become an active member of the community and consider giving back by making
contributions.


Licensing
---------

a) Intel source code is being released under the Apache 2.0 license.

b) Additional libraries used with COSBench have their own licensing; refer to 3rd-party-licenses.pdf for details.


Distribution Packages
---------------------

Please refer to "DISTRIBUTIONS.md" to get the link for distribution packages.


Installation & Usage
--------------------

Please refer to "COSBenchUserGuide.pdf" for details.


Adaptor Development
-------------------
If needed, adaptors can be developed for new storage services; please refer to "COSBenchAdaptorDevGuide.pdf" for details.


Build
-----
If a build from source code is needed, please refer to BUILD.md for details.


Resources
---------

Wiki: (https://github.com/intel-cloud/cosbench/wiki)

Issue tracking: (https://github.com/intel-cloud/cosbench/issues)

Mailing list: (http://cosbench.1094679.n5.nabble.com/)


*Other names and brands may be claimed as the property of others.


Other related projects
----------------------
COSBench-Workload-Generator: (https://github.com/giteshnandre/COSBench-Workload-Generator)

COSBench-Plot: (https://github.com/icclab/cosbench-plot)

COSBench-Appliance: (https://susestudio.com/a/8Kp374/cosbench)

COSBench Ansible Playbook:

- (http://www.ksingh.co.in/blog/2016/05/29/deploy-cosbench-using-ansible/)
- (https://github.com/ksingh7/ansible-role-cosbench)
- (https://galaxy.ansible.com/ksingh7/cosbench/)


= END =
