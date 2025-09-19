/*
 * This file is part of the KubeVirt project
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * Copyright The KubeVirt Authors.
 *
 */

package vsock

import (
	"fmt"
	"io"
	"net"
	"os"
	"os/signal"

	"github.com/spf13/cobra"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1 "kubevirt.io/api/core/v1"
	"kubevirt.io/client-go/kubecli"
	"kubevirt.io/client-go/log"

	"kubevirt.io/kubevirt/pkg/virtctl/clientconfig"
	"kubevirt.io/kubevirt/pkg/virtctl/templates"
)

const (
	portFlag           = "port"
	portFlagShort      = "p"
	tlsFlag            = "tls"
	tlsFlagShort       = "t"
	localPortFlag      = "local-port"
	localPortFlagShort = "l"
	addressFlag        = "address"
	addressFlagShort   = "a"
)

type VSockOptions struct {
	Port      uint32
	UseTLS    bool
	LocalPort uint32
	Address   string
}

type VSock struct {
	options VSockOptions
}

func NewCommand() *cobra.Command {
	c := &VSock{
		options: VSockOptions{
			Port:      22,
			UseTLS:    true,
			LocalPort: 0, // Will be assigned automatically if not specified
			Address:   "localhost",
		},
	}

	cmd := &cobra.Command{
		Use:     "vsock (VM|VMI)",
		Short:   "Create a local port forward to a virtual machine instance via VSOCK.",
		Long:    "Listen on a local TCP port and forward connections to a virtual machine instance via VSOCK. This enables standard tools like SSH to work over the high-performance VSOCK transport.",
		Example: usage(),
		Args:    cobra.ExactArgs(1),
		RunE:    c.Run,
	}

	cmd.Flags().Uint32VarP(&c.options.Port, portFlag, portFlagShort, c.options.Port,
		fmt.Sprintf("--%s=22: Target port on the VM to connect to via VSOCK", portFlag))
	cmd.Flags().BoolVarP(&c.options.UseTLS, tlsFlag, tlsFlagShort, c.options.UseTLS,
		fmt.Sprintf("--%s=true: Enable/disable TLS for the VSOCK connection", tlsFlag))
	cmd.Flags().Uint32VarP(&c.options.LocalPort, localPortFlag, localPortFlagShort, c.options.LocalPort,
		fmt.Sprintf("--%s=0: Local port to listen on (0 = auto-assign)", localPortFlag))
	cmd.Flags().StringVarP(&c.options.Address, addressFlag, addressFlagShort, c.options.Address,
		fmt.Sprintf("--%s=localhost: Local address to bind to", addressFlag))

	cmd.SetUsageTemplate(templates.UsageTemplate())
	return cmd
}

func (v *VSock) Run(cmd *cobra.Command, args []string) error {
	virtClient, namespace, _, err := clientconfig.ClientAndNamespaceFromContext(cmd.Context())
	if err != nil {
		return err
	}

	kind, namespace, name, err := parseTarget(args[0], namespace)
	if err != nil {
		return fmt.Errorf("failed to parse target: %v", err)
	}

	// Auto-assign local port if not specified
	if v.options.LocalPort == 0 {
		// Find an available port
		listener, err := net.Listen("tcp", v.options.Address+":0")
		if err != nil {
			return fmt.Errorf("failed to find available port: %v", err)
		}
		v.options.LocalPort = uint32(listener.Addr().(*net.TCPAddr).Port)
		listener.Close()
	}

	vsoOptions := &v1.VSOCKOptions{
		TargetPort: v.options.Port,
		UseTLS:     &v.options.UseTLS,
	}

	return v.startListening(virtClient, namespace, name, kind, vsoOptions, cmd)
}

func (v *VSock) startListening(virtClient kubecli.KubevirtClient, namespace, name, kind string, options *v1.VSOCKOptions, cmd *cobra.Command) error {
	// Validate VMI is running first
	err := v.validateVMI(virtClient, namespace, name, kind, cmd)
	if err != nil {
		return err
	}

	// Start listening on the local address
	listenAddr := fmt.Sprintf("%s:%d", v.options.Address, v.options.LocalPort)
	listener, err := net.Listen("tcp", listenAddr)
	if err != nil {
		return fmt.Errorf("failed to listen on %s: %v", listenAddr, err)
	}
	defer listener.Close()

	log.Log.Infof("Listening on %s, forwarding to %s/%s VSOCK port %d (TLS: %t)",
		listenAddr, kind, name, options.TargetPort, *options.UseTLS)

	// Set up signal handling for graceful shutdown
	sigChan := make(chan os.Signal, 1)
	signal.Notify(sigChan, os.Interrupt)

	// Accept connections in a goroutine
	connChan := make(chan net.Conn)
	errChan := make(chan error)

	go func() {
		for {
			conn, err := listener.Accept()
			if err != nil {
				errChan <- err
				return
			}
			connChan <- conn
		}
	}()

	// Handle connections and signals
	for {
		select {
		case <-sigChan:
			log.Log.Info("Received interrupt, shutting down...")
			return nil
		case err := <-errChan:
			return fmt.Errorf("listener error: %v", err)
		case conn := <-connChan:
			go v.handleConnection(conn, virtClient, namespace, name, options)
		}
	}
}

func (v *VSock) handleConnection(localConn net.Conn, virtClient kubecli.KubevirtClient, namespace, name string, options *v1.VSOCKOptions) {
	defer localConn.Close()

	log.Log.Infof("Handling new connection, opening VSOCK tunnel to port %d", options.TargetPort)

	// Create VSOCK connection to the VM
	stream, err := virtClient.VirtualMachineInstance(namespace).VSOCK(name, options)
	if err != nil {
		log.Log.Errorf("Failed to create VSOCK connection: %v", err)
		return
	}
	defer stream.AsConn().Close()

	vsockConn := stream.AsConn()

	// Copy data bidirectionally
	errChan := make(chan error, 2)

	go func() {
		_, err := io.Copy(vsockConn, localConn)
		errChan <- err
	}()

	go func() {
		_, err := io.Copy(localConn, vsockConn)
		errChan <- err
	}()

	// Wait for either direction to complete
	err = <-errChan
	if err != nil {
		log.Log.V(2).Infof("Connection ended: %v", err)
	}
	log.Log.V(2).Infof("VSOCK tunnel closed for port %d", options.TargetPort)
}

func (v *VSock) validateVMI(virtClient kubecli.KubevirtClient, namespace, name, kind string, cmd *cobra.Command) error {
	// Get the VMI based on the kind
	var vmi *v1.VirtualMachineInstance
	var err error

	switch kind {
	case "vm":
		vm, err := virtClient.VirtualMachine(namespace).Get(cmd.Context(), name, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("failed to get VM %s: %v", name, err)
		}
		if vm.Status.PrintableStatus != v1.VirtualMachineStatusRunning {
			return fmt.Errorf("VM %s is not running", name)
		}
		vmi, err = virtClient.VirtualMachineInstance(namespace).Get(cmd.Context(), name, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("failed to get VMI for VM %s: %v", name, err)
		}
	case "vmi":
		vmi, err = virtClient.VirtualMachineInstance(namespace).Get(cmd.Context(), name, metav1.GetOptions{})
		if err != nil {
			return fmt.Errorf("failed to get VMI %s: %v", name, err)
		}
	default:
		return fmt.Errorf("unsupported resource type: %s", kind)
	}

	if vmi.Status.Phase != v1.Running {
		return fmt.Errorf("VMI %s is not running", name)
	}

	return nil
}

func parseTarget(arg, fallbackNamespace string) (kind, namespace, name string, err error) {
	// Parse target in format: kind/name or kind/name/namespace
	// Supported formats:
	// - vm/myvm
	// - vmi/myvmi
	// - vm/myvm/mynamespace
	// - vmi/myvmi/mynamespace

	parts := split(arg, "/")
	if len(parts) < 2 {
		return "", "", "", fmt.Errorf("invalid target format, expected: kind/name or kind/name/namespace")
	}

	kind = parts[0]
	name = parts[1]
	namespace = fallbackNamespace

	if len(parts) == 3 {
		namespace = parts[2]
	}

	if kind != "vm" && kind != "vmi" {
		return "", "", "", fmt.Errorf("invalid kind '%s', supported: vm, vmi", kind)
	}

	if name == "" {
		return "", "", "", fmt.Errorf("name cannot be empty")
	}

	return kind, namespace, name, nil
}

func split(s, sep string) []string {
	// Simple string split function
	var result []string
	start := 0

	for i := 0; i < len(s); i++ {
		if s[i:i+1] == sep {
			if start < i {
				result = append(result, s[start:i])
			}
			start = i + 1
		}
	}

	if start < len(s) {
		result = append(result, s[start:])
	}

	return result
}

func usage() string {
	return fmt.Sprintf(`  # Forward to VMI 'myvmi' on default port 22 (auto-assign local port):
  {{ProgramName}} vsock vmi/myvmi

  # Forward local port 2222 to VSOCK port 22:
  {{ProgramName}} vsock vmi/myvmi --%s=2222

  # SSH over VSOCK using port forwarding:
  {{ProgramName}} vsock vmi/myvmi --%s=2222 &
  ssh user@localhost -p 2222

  # Forward to VM 'myvm' on port 80 without TLS:
  {{ProgramName}} vsock vm/myvm --%s=80 --%s=false

  # Forward to VMI 'myvmi' in 'production' namespace:
  {{ProgramName}} vsock vmi/myvmi/production

  # Listen on all interfaces on port 2222:
  {{ProgramName}} vsock vm/myvm --%s=0.0.0.0 --%s=2222 --%s=443 --%s=false

  # Auto-assign port and display connection info:
  {{ProgramName}} vsock vmi/myvmi --%s=8080`,
		localPortFlag, localPortFlag, portFlag, tlsFlag, addressFlag, localPortFlag, portFlag, tlsFlag, localPortFlag)
}
