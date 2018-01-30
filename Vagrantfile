# -*- mode: ruby -*-
# vi: set ft=ruby :

# All Vagrant configuration is done below. The "2" in Vagrant.configure
# configures the configuration version (we support older styles for
# backwards compatibility). Please don't change it unless you know what
# you're doing.
Vagrant.configure("2") do |config|
  # The most common configuration options are documented and commented below.
  # For a complete reference, please see the online documentation at
  # https://docs.vagrantup.com.

  # Every Vagrant development environment requires a box. You can search for
  # boxes at https://atlas.hashicorp.com/search.
  #config.vm.box = "base"
  #config.vm.box = "hashicorp/precise64"
  config.vm.box = "ubuntu/xenial64"

  # Disable automatic box update checking. If you disable this, then
  # boxes will only be checked for updates when the user runs
  # `vagrant box outdated`. This is not recommended.
  # config.vm.box_check_update = false

  # Create a forwarded port mapping which allows access to a specific port
  # within the machine from a port on the host machine. In the example below,
  # accessing "localhost:8080" will access port 80 on the guest machine.
  # config.vm.network "forwarded_port", guest: 80, host: 8080

  # Create a private network, which allows host-only access to the machine
  # using a specific IP.
  # config.vm.network "private_network", ip: "192.168.33.10"

  # Create a public network, which generally matched to bridged network.
  # Bridged networks make the machine appear as another physical device on
  # your network.
  # config.vm.network "public_network"

  # Share an additional folder to the guest VM. The first argument is
  # the path on the host to the actual folder. The second argument is
  # the path on the guest to mount the folder. And the optional third
  # argument is a set of non-required options.
  # config.vm.synced_folder "../data", "/vagrant_data"

  # Provider-specific configuration so you can fine-tune various
  # backing providers for Vagrant. These expose provider-specific options.
  # Example for VirtualBox:
  #
   config.vm.provider "virtualbox" do |vb|
  #   # Display the VirtualBox GUI when booting the machine
  #   vb.gui = true
  #
  #   # Customize the amount of memory on the VM:
     vb.memory = "2048"
   end
  #
  # View the documentation for the provider you are using for more
  # information on available options.

  # Define a Vagrant Push strategy for pushing to Atlas. Other push strategies
  # such as FTP and Heroku are also available. See the documentation at
  # https://docs.vagrantup.com/v2/push/atlas.html for more information.
  # config.push.define "atlas" do |push|
  #   push.app = "YOUR_ATLAS_USERNAME/YOUR_APPLICATION_NAME"
  # end

  # Enable provisioning with a shell script. Additional provisioners such as
  # Puppet, Chef, Ansible, Salt, and Docker are also available. Please see the
  # documentation for more information about their specific syntax and use.
   config.vm.provision "shell", inline: <<-SHELL
     #apt-get install software-properties-common python-software-properties
     #add-apt-repository ppa:jonathonf/gcc-7.1
     apt-get update
     apt-get install -y git
     apt-get install -y build-essential
     apt-get install -y cmake
     sudo apt-get install -y libapr1 libapr1-dev libaprutil1-dev sqlite libsqlite3-dev
     sudo apt-get install -y flex bison python
     wget http://apt.typesafe.com/repo-deb-build-0002.deb
     sudo dpkg -i repo-deb-build-0002.deb
     sudo apt-get update
     sudo apt-get install -y sbt openjdk-8-jdk
   SHELL
  #config.vm.provision :file, source: "../box-of-pain", destination: "box-of-pain"
  #config.vm.provision :shell, path: "box.sh", privileged: false

  config.vm.provision "shell", privileged: false, inline: <<-SHELL
    echo "deb https://dl.bintray.com/sbt/debian /" | sudo tee -a /etc/apt/sources.list.d/sbt.list
    sudo apt-key adv --keyserver hkp://keyserver.ubuntu.com:80 --recv 2EE0EA64E40A89B84B2DF73499E82A75642AC823
    sudo apt-get update
    sudo apt-get install sbt
    git clone https://github.com/palvaro/molly.git
    cd molly/lib
    git clone https://github.com/Z3Prover/z3.git
    cd ..
    export PYTHON=python3
    make
    echo "export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:~/molly/lib/z3/build/z3-dist/lib/:~/molly/lib/c4/build/src/lib/libc4.so" > env.sh
  SHELL
end
