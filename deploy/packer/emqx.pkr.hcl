packer {
  required_plugins {
    amazon = {
      version = ">= 1.2.8"
      source  = "github.com/hashicorp/amazon"
    }
  }
}

variable "arch" {
  default = "amd64"
}

variable "emqx_version" {
  default = ""
}

variable "emqx_package_file_path" {
  default = ""
}

locals {

  setup_script = <<EOF
#!/bin/bash

cat <<END | sudo tee /etc/security/limits.d/99-emqx.conf
* soft    nofile  1024000
* hard    nofile  1024000
* soft    nproc   unlimited
* hard    nproc   unlimited
* soft    core    unlimited
* hard    core    unlimited
* soft    memlock unlimited
* hard    memlock unlimited
END

cat <<END | sudo tee /etc/sysctl.d/99-emqx.conf
# File system
fs.aio-max-nr = 1048576
fs.file-max = 4194304
fs.nr_open = 2097152

# TCP
net.ipv4.ip_local_port_range = 1025 65525
net.ipv4.tcp_max_syn_backlog=65536
net.ipv4.tcp_fin_timeout=15
net.ipv4.tcp_keepalive_time=180
net.ipv4.tcp_keepalive_intvl=60
net.ipv4.tcp_keepalive_probes=3
net.ipv4.tcp_rmem=4096 87380 16777216
net.ipv4.tcp_wmem=4096 65536 16777216
net.ipv4.tcp_mem=786432 1048576 1572864
net.ipv4.tcp_max_tw_buckets=1048576
net.core.somaxconn=65535
net.core.netdev_max_backlog=32768
net.core.rmem_default=262144
net.core.wmem_default=262144
net.core.rmem_max=16777216
net.core.wmem_max=16777216
net.core.optmem_max=40960

# vm
vm.dirty_ratio = 30
vm.dirty_background_ratio = 10
vm.dirty_expire_centisecs = 500
vm.dirty_writeback_centisecs = 100
vm.mmap_min_addr = 65536
vm.swappiness = 1
vm.zone_reclaim_mode = 0
vm.overcommit_memory = 1
vm.min_free_kbytes = 262144
END

EOF
}

source "amazon-ebs" "emqx-enterprise" {
  instance_type               = var.arch == "arm64" ? "t4g.micro" : "t3.micro"
  ssh_username                = "ubuntu"
  ami_name                    = "emqx-enterprise-${var.emqx_version}-ubuntu22.04-${var.arch}"
  ami_description             = "emqx-enterprise-${var.emqx_version}-ubuntu22.04-${var.arch}"
  associate_public_ip_address = true

  source_ami_filter {
    filters = {
      name                = "ubuntu/images/*ubuntu-jammy-22.04-${var.arch}-server-*"
      root-device-type    = "ebs"
      virtualization-type = "hvm"
    }

    most_recent = true
    owners      = ["099720109477"]
  }

  run_tags = {
    Product   = "marketplace-ami-build"
    CreatedBy = "packer"
    Arch      = var.arch
    Version   = var.emqx_version
  }
}

build {
  sources = ["sources.amazon-ebs.emqx-enterprise"]
  provisioner "file" {
    source      = var.emqx_package_file_path
    destination = "/tmp/emqx-enterprise.deb"
  }

  provisioner "shell" {
    inline = [
      local.setup_script,
      "sudo apt-get install -y --no-install-recommends /tmp/emqx-enterprise.deb",
      "sudo truncate -s 0 /root/.ssh/authorized_keys",
      "truncate -s 0 /home/ubuntu/.ssh/authorized_keys",
    ]
  }
}
