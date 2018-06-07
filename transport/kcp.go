package transport

import (
	"net"
	"github.com/hashicorp/raft"
	"time"
	"github.com/xtaci/smux"
	"github.com/xtaci/kcp-go"
	"errors"
	"io"
)

var (
	errNotAdvertisable = errors.New("local bind address is not advertisable")
	errNotTCP          = errors.New("local address is not a TCP address")
)
// smuxConf is the config for smux server and client
func smuxConf() (conf *smux.Config) {
	conf = smux.DefaultConfig()
	// TODO: potentially tweak timeouts
	conf.KeepAliveInterval = time.Second * 5
	conf.KeepAliveTimeout = time.Second * 13
	return
}

type KCPStreamLayer struct {
	conn      net.Conn
	sess      *smux.Session
	bs        *smux.Stream //base stream
	advertise net.Addr
	listener  *kcp.Listener
}

func NewKCPTransport(
	bindAddr string,
	advertise net.Addr,
	maxPool int,
	timeout time.Duration,
	logOutput io.Writer,
) (*raft.NetworkTransport, error) {
	return newKCPTransport(bindAddr, advertise, func(stream raft.StreamLayer) *raft.NetworkTransport {
		return raft.NewNetworkTransport(stream, maxPool, timeout, logOutput)
	})
}

func newKCPTransport(bindAddr string,
	advertise net.Addr,
	transportCreator func(stream raft.StreamLayer) *raft.NetworkTransport) (*raft.NetworkTransport, error) {
	// Try to bind
	list, err := kcp.Listen(bindAddr)
	if err != nil {
		return nil, err
	}

	// Create stream
	stream := &KCPStreamLayer{
		advertise: advertise,
		listener:  list.(*kcp.Listener),
	}

	/*// Verify that we have a usable advertise address
	addr, ok := stream.Addr().(*net.TCPAddr)
	if !ok {
		list.Close()
		return nil, errNotTCP
	}*/
	/*if addr.IP.IsUnspecified() {
		list.Close()
		return nil, errNotAdvertisable
	}*/

	// Create the network transport
	trans := transportCreator(stream)
	return trans, nil
}

func (ksl *KCPStreamLayer) Accept() (net.Conn, error) {
	maConn, err := ksl.listener.Accept()
	if err != nil {
		return nil, err
	}

	// maConn is wrapping an accepted KCP session.
	smuxSess, err := smux.Server(maConn, smuxConf())
	if err != nil {
		maConn.Close()
		return nil, err
	}

	stream, err := smuxSess.AcceptStream()
	if err != nil {
		if stream != nil {
			stream.Close()
		}
		smuxSess.Close()
		maConn.Close()
		return nil, err
	}

	return stream, err
}

func (ksl *KCPStreamLayer) Close() error {
	ksl.bs.Close()
	ksl.sess.Close()
	return ksl.conn.Close()
}

func (ksl *KCPStreamLayer) Addr() net.Addr {
	return ksl.listener.Addr()
}

func (ksl *KCPStreamLayer) Dial(address raft.ServerAddress, timeout time.Duration) (net.Conn, error) {
	var err error
	ksl.conn, err = kcp.Dial(string(address))
	if err != nil {
		return nil, err
	}

	ksl.sess, err = smux.Client(ksl.conn, smuxConf())
	if err != nil {
		ksl.conn.Close()
		return nil, err
	}

	// TODO: In the initial version use this to emulate tcp.
	ksl.bs, err = ksl.sess.OpenStream()
	return ksl.bs, err
}
