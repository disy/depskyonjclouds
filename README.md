DepSky on JClouds
=================

This project has been concluded from http://code.google.com/p/depsky/.

## Purpose

This project is set up to adapt depsky in such a manner, that it uses jclouds as a cloudprovider backend.
During this process, we try to create a more user-friendly interface for easy embedded use in other programs.

We want to exclude some security measures, such as secret key sharing, since for our later use-case it is not necessary. Due to the remodelling of the system-architecture, we believe that it can easily be implemented later on.

## Adaptions Made

* Driver usage unnecessary when using JClouds -> removed.
* New configuration protocol using yaml
* Code refactorings such as:
   - Introducing new interfaces
   - Getting rid of public access to variables
   - Removing unnecessary code
   - Main method not needed ( we do not intend to use this as a standalone client, rather than a library client )
   - CloudManager fully relies on JClouds
   - Reduced data in some objects
   - CloudManagers are now callables and handle by an ExecutorService, instead of being Threads.
* Since JClouds gives the opportunity to differ between "blobs" and "containers" and also allows users to add metadata directly to a "blob", we
   - Introduced a new paradigm, to retrieve metadata first and only if metadata is available, download (read) the payload. That leads to more distinctions in
     the operation set, such as DepSkyCloudMangager.GET_META.
   - can now upload multiple dataunits to one container

## Future Adaptions
The last changes regarding refactorings on the DataUnit side and also the handling of MetaData, leads to the renewal of the CloudManager workflow, aswell as
re-programming the read and write operations on the client side. Since the metadata is added directly to the blobs, we have to change the DepSkyManager aswell.

## License
This work released in the Apache License 2.0.

## Involved People
This project is supervised by Sebastian Grad from the Distributed System Group at the University of Konstanz.

* Sebastian Grad (Project Lead)
* Andreas Rain (Reprogramming & Redesign of the System)
