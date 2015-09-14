ant package
cd ~/workspace/github/azkaban-plugins/dist/sfnotify/packages/
rsync -avr --exclude azkaban-sfnotify-2.6.4.tar.gz  * root@sf51:/usr/local/azkaban/azkaban-web-server-2.6.4/plugins/alerter/sfnotify/

