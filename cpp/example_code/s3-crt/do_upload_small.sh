#!/bin/bash 

./run_put_object_directory crtbucket /bryck/smallfiles/dir1 1 &
./run_put_object_directory crtbucket /bryck/smallfiles/dir2 1 &
./run_put_object_directory crtbucket /bryck/smallfiles/dir3 1 &
./run_put_object_directory crtbucket /bryck/smallfiles/dir4 1 &
./run_put_object_directory crtbucket /bryck/smallfiles/dir5 1 &
./run_put_object_directory crtbucket /bryck/smallfiles/dir6 1 &
./run_put_object_directory crtbucket /bryck/smallfiles/dir7 1 &
./run_put_object_directory crtbucket /bryck/smallfiles/dir8 1 &
./run_put_object_directory crtbucket /bryck/smallfiles/dir9 1 &
./run_put_object_directory crtbucket /bryck/smallfiles/dir10 1 &


wait
