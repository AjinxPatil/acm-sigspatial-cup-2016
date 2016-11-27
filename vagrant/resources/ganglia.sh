#!/bin/bash
#    Copyright 2015 Google, Inc.
#
#    Licensed under the Apache License, Version 2.0 (the "License");
#    you may not use this file except in compliance with the License.
#    You may obtain a copy of the License at
#
#        http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS,
#    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#    See the License for the specific language governing permissions and
#    limitations under the License.
#
#	Reference: https://github.com/GoogleCloudPlatform/dataproc-initialization-actions/blob/master/ganglia/ganglia.sh
 set -x -e

apt-get update && apt-get install -y ganglia-monitor

DEFAULT_MASTER_IP="192.168.200.10"
SYSTEM_IP="$(ifconfig | grep -A 1 'eth1' | tail -1 | cut -d ':' -f 2 | cut -d ' ' -f 1)"

sed -e "/name = \"unspecified\" /s/unspecified/geosparky-cluster/" -i /etc/ganglia/gmond.conf
sed -e '/mcast_join /s/^  /  #/' -i /etc/ganglia/gmond.conf
sed -e '/bind /s/^  /  #/' -i /etc/ganglia/gmond.conf

if [[ "$SYSTEM_IP" == "$DEFAULT_MASTER_IP" ]]; then
    # Only run on the master node
    # Install dependencies needed for ganglia
    DEBIAN_FRONTEND=noninteractive apt install -y rrdtool gmetad ganglia-webfrontend
    cp /etc/ganglia-webfrontend/apache.conf /etc/apache2/sites-enabled/ganglia.conf

    sed -i "s/\"my cluster\"/\"geosparky-cluster\" 30/" /etc/ganglia/gmetad.conf
    sed -e '/udp_send_channel {/a\  host = localhost' -i /etc/ganglia/gmond.conf

    service ganglia-monitor restart && service gmetad restart && service apache2 restart

else

    sed -e "/udp_send_channel {/a\  host = $DEFAULT_MASTER_IP" -i /etc/ganglia/gmond.conf
    sed -i '/udp_recv_channel {/,/}/d' /etc/ganglia/gmond.conf
    usermod -aG sudo ganglia
    service ganglia-monitor restart

fi

