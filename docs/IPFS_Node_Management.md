Set up IPFS node
================

These instructions will install an IPFS node on Linux to be managed by the `systemd` utility.

- Download [latest Kubo (go-ipfs) from Github](https://github.com/ipfs/kubo)

        $ wget [.TAR.GZ URL]

- Unzip and run the install script

        $ tar -xf go-ipfs-[VERSION].tar.gz
        $ cd go-ipfs
        $ ./install.sh
        
- Initialize the IPFS node

        $ ipfs init

- If you're planning to parse large datasets, consider increasing the number of files that can be open simultaneously, and other limits

        $ vim /etc/security/limits.conf
        
- by adding the following to the bottom of `limits.conf`

        #
        # Remove per-process limits for root user (which launches the IPFS daemon)
        #
        root            soft    nofile          unlimited
        root            hard    nofile          unlimited
        root            soft    nproc           unlimited
        root            hard    nproc           unlimited
        root            soft    memlock         unlimited
        root            hard    memlock         unlimited
        
- Create a systemd config file for IPFS

        $ vim /etc/systemd/system/ipfs.service
        
- by adding the following to `ipfs.service`. The `--enable-namesys-pubsub` flag is necessary for keeping published datasets and STAC keys valid.

        [Unit]
        Description=IPFS Daemon
        After=syslog.target network.target remote-fs.target nss-lookup.target
        
        [Service]
        Type=simple
        ExecStart=/usr/local/bin/ipfs daemon --enable-namesys-pubsub
        Restart=always
        RestartSec=0
        User=root
        
        [Install]
        WantedBy=multi-user.target
        
- and enable it so it starts at boot

        $ systemctl enable ipfs.service
        
- Start the IPFS daemon

        $ systemctl start ipfs
        
- Check its status to ensure it's running

        $ systemctl status ipfs

- Verify the process limits are correct

        $ prlimit --pid [PID OF IPFS DAEMON]


Remote IPFS Daemon
------------------

Although not necessary for running a cycle, it is worth noting that it is possible to use an SSH tunnel to automatically forward IPFS operations to a remote node. This allows for a workflow where CPU and RAM intensive data processing is performed on a separate system while still directly adding data to the remote IPFS node through the SSH tunnel. This code is not checked into the repository.

[This script](https://gist.github.com/Kubuxu/0cafd6dc71114349875827c2c379fa1f) by Gist user Kubuxu can be used to launch the SSH tunnel. `YOUR REMOTE HOST HERE` should be replaced by the IP address of the IPFS node where data will be added. This should be launched before launching any other code and should be left running in the background.

[Python logging module](https://docs.python.org/3/library/logging.html)

[logging module's root log](https://docs.python.org/3/library/logging.html#logging.getLogger)

[data retrieval, transformation, and storage cycle](https://en.wikipedia.org/wiki/Extract,_transform,_load)

[web scraper](https://en.wikipedia.org/wiki/Web_scraping)

[IPFS](https://ipfs.tech/)