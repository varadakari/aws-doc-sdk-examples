#!/bin/bash 

./run_put_object_directory crtbucket /bryck/test/dir1 1 &
./run_put_object_directory crtbucket /bryck/test/dir2 1 &
./run_put_object_directory crtbucket /bryck/test/dir3 1 &
./run_put_object_directory crtbucket /bryck/test/dir4 1 &
./run_put_object_directory crtbucket /bryck/test/dir5 1 &
./run_put_object_directory crtbucket /bryck/test/dir6 1 &
./run_put_object_directory crtbucket /bryck/test/dir7 1 &
./run_put_object_directory crtbucket /bryck/test/dir8 1 &
./run_put_object_directory crtbucket /bryck/test/dir9 1 &
./run_put_object_directory crtbucket /bryck/test/dir10 1 &


wait
