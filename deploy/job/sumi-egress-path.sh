# sumi-egress-path — keep `pip --user` console scripts runnable by name.
#
# The shared terminal launches `bash -l`; /etc/profile resets PATH to the
# stock value, dropping the user-site bin dir the provisioner puts on the
# container PATH when job egress is enabled (it leads with
# $HOME/.local/bin so installs land in the persistent workspace).
#
# This snippet re-adds the directory only when the egress socket mount is
# actually present, so the explicit-egress-disabled and agent-image
# contracts stay byte-identical, and only when the directory exists.
sumi_egress_home=${HOME:-/workspace}
if [ -S /run/sumi/egress/proxy.sock ] && [ -d "$sumi_egress_home/.local/bin" ]; then
    case ":$PATH:" in
        *":$sumi_egress_home/.local/bin:"*) ;;
        *) PATH="$sumi_egress_home/.local/bin:$PATH" ;;
    esac
    export PATH
fi
unset sumi_egress_home
