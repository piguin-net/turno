package com.example;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.charset.Charset;
import java.util.Date;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.function.Consumer;

public class UdpConnectionManager implements AutoCloseable
{
    private final DatagramChannel channel;
    private final Thread receiver;
    private final Thread keepalive;
    private int interval = 1_000;
    private int keepaliveTimeout = 3_000;
    // private int connectionTimeout = 3 * 60_000;
    private boolean active = false;
    private List<Consumer<InetSocketAddress>> connectEventListener = new ArrayList<>();
    private List<Consumer<InetSocketAddress>> disconnectEventListener = new ArrayList<>();
    private List<Consumer<Pair<InetSocketAddress, byte[]>>> receiveEventListener = new ArrayList<>();
    private List<Consumer<Pair<InetSocketAddress, Exception>>> errorEventListener = new ArrayList<>();
    private Map<InetSocketAddress, Long> peers = new HashMap<>();

    public UdpConnectionManager(Integer port) throws IOException {
        this.channel = DatagramChannel.open();
        this.channel.configureBlocking(false);
        this.channel.socket().bind(new InetSocketAddress(port));
        this.receiver = new Thread(() -> {
            while (this.active) {
                try {
                    ByteBuffer buffer = ByteBuffer.allocate(65535);
                    SocketAddress socket = this.channel.receive(buffer);
                    if (socket instanceof InetSocketAddress packet) {
                        InetSocketAddress peer = new InetSocketAddress(
                            packet.getAddress(),
                            packet.getPort()
                        );
                        if (!peers.containsKey(peer) || peers.get(peer) < 0) {
                            this.connectEventListener.forEach(
                                listener -> new Thread(
                                    () -> listener.accept(peer),
                                    String.format(
                                        "UdpHolePunching ConnectEventListenerThread[%s:%d]",
                                        peer.getAddress().getHostAddress(),
                                        peer.getPort()
                                    )
                                ).start()
                            );
                        }
                        peers.put(peer, new Date().getTime());
                        if (buffer.flip().limit() > 0) {
                            byte[] data = new byte[buffer.limit()];
                            buffer.get(data);
                            this.receiveEventListener.stream().forEach(
                                listener -> new Thread(
                                    () -> listener.accept(Pair.of(
                                        new InetSocketAddress(packet.getAddress(), packet.getPort()),
                                        data
                                    )),
                                    String.format(
                                        "UdpHolePunching ReceiveEventListenerThread[%s:%d]",
                                        peer.getAddress().getHostAddress(),
                                        peer.getPort()
                                    )
                                ).start()
                            );
                        }
                    }
                    Thread.sleep(1);
                } catch (Exception e) {
                    if (this.active) {
                        this.errorEventListener.stream().forEach(
                            listener -> new Thread(
                                () -> listener.accept(Pair.of(null, e)),
                                "UdpHolePunching ErrorEventListenerThread"
                            ).start()
                        );
                    }
                }
            }
        }, "UdpHolePunching ReceiverThread");
        this.keepalive = new Thread(() -> {
            while (this.active) {
                List<Entry<InetSocketAddress, Long>> entries = this.peers.entrySet().stream().filter(
                    entry -> entry.getValue() > 0
                ).filter(
                    entry -> new Date().getTime() - entry.getValue() > this.keepaliveTimeout
                ).toList();
                entries.forEach(
                    entry -> {
                        this.peers.remove(entry.getKey());
                        this.disconnectEventListener.forEach(
                            listener -> new Thread(
                                () -> listener.accept(entry.getKey()),
                                String.format(
                                    "UdpHolePunching DisconnectEventListenerThread[%s:%d]",
                                    entry.getKey().getAddress().getHostAddress(),
                                    entry.getKey().getPort()
                                )
                            ).start()
                        );
                    }
                );
                this.peers.keySet().forEach(peer -> {
                    try {
                        this.channel.send(ByteBuffer.allocate(0), peer);
                    } catch (Exception e) {
                        if (this.active) {
                            this.errorEventListener.stream().forEach(
                                listener -> new Thread(
                                    () -> listener.accept(Pair.of(peer, e)),
                                    String.format(
                                        "UdpHolePunching ErrorEventListenerThread[%s:%d]",
                                        peer.getAddress().getHostAddress(),
                                        peer.getPort()
                                    )
                                ).start()
                            );
                        }
                    }
                });
                try {
                    Thread.sleep(this.interval);
                } catch (InterruptedException e) {
                    if (this.active) {
                        throw new RuntimeException(e);
                    }
                }
            }
        }, "UdpHolePunching KeepAliveThread");
    }

    public void start() {
        this.active = true;
        this.receiver.start();
        this.keepalive.start();
    }

    public UdpConnectionManager setKeepAliveInterval(int interval) {
        this.interval = interval;
        return this;
    }

    public UdpConnectionManager onConnect(Consumer<InetSocketAddress> listener) {
        this.connectEventListener.add(listener);
        return this;
    }

    public UdpConnectionManager onDisconnect(Consumer<InetSocketAddress> listener) {
        this.disconnectEventListener.add(listener);
        return this;
    }

    public UdpConnectionManager onReceive(Consumer<Pair<InetSocketAddress, byte[]>> listener) {
        this.receiveEventListener.add(listener);
        return this;
    }

    public UdpConnectionManager onError(Consumer<Pair<InetSocketAddress, Exception>> listener) {
        this.errorEventListener.add(listener);
        return this;
    }

    public void connectAsync(InetSocketAddress peer) throws InterruptedException {
        if (!this.peers.containsKey(peer)) {
            this.peers.put(peer, -1 * new Date().getTime());
        }
    }

    public void connect(InetSocketAddress peer) throws InterruptedException {
        this.connectAsync(peer);
        while (this.peers.get(peer) < 0) {
            Thread.sleep(100);
        }
    }

    public void disconnect(InetSocketAddress peer) {
        this.peers.remove(peer);
    }

    public void send(byte[] data, InetSocketAddress peer) throws IOException, InterruptedException {
        ByteBuffer buffer = ByteBuffer.wrap(data);
        this.connect(peer);
        this.channel.send(buffer, peer);
    }

    public void send(byte[] data) throws IOException, InterruptedException {
        for (InetSocketAddress peer: this.peers.keySet()) {
            ByteBuffer buffer = ByteBuffer.wrap(data);
            this.connect(peer);
            this.channel.send(buffer, peer);
        }
    }

    @Override
    public void close() throws Exception {
        this.active = false;
        this.channel.close();
        this.receiver.join();
        this.keepalive.join();
    }

    public static void main( String[] args ) throws Exception
    {
        Integer port = Integer.getInteger("p2p.port", 9625);
        Integer interval = Integer.getInteger("p2p.keepalive.interval", 1000);

        try (UdpConnectionManager p2p = new UdpConnectionManager(port);) {
            p2p.setKeepAliveInterval(
                interval
            ).onConnect(peer -> {
                System.out.println(String.format(
                    "[onConnect] %s:%d",
                    peer.getAddress().getHostAddress(),
                    peer.getPort()
                ));
            }).onDisconnect(peer -> {
                System.out.println(String.format(
                    "[onDisconnect] %s:%d retry...",
                    peer.getAddress().getHostAddress(),
                    peer.getPort()
                ));
                try {
                    p2p.connect(peer);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }).onReceive(receive -> {
                String message = new String(receive.getValue());
                System.out.println(String.format(
                    "[onReceive] %s:%d %s",
                    receive.getKey().getAddress(),
                    receive.getKey().getPort(),
                    message
                ));
            }).onError(pair -> {
                if (pair.getKey() == null) {
                    System.out.println(String.format(
                        "[onError] %s(%s)",
                        pair.getValue().getClass().getName(),
                        pair.getValue().getMessage()
                    ));
                } else {
                    System.out.println(String.format(
                        "[onError] %s:%d %s(%s)",
                        pair.getKey().getAddress().getHostAddress(),
                        pair.getKey().getPort(),
                        pair.getValue().getClass().getName(),
                        pair.getValue().getMessage()
                    ));
                }
            }).start();

            for (String arg: args) {
                InetSocketAddress addr = new InetSocketAddress(arg, port);
                p2p.connect(addr);
            }

            try (BufferedReader input = new BufferedReader(new InputStreamReader(System.in));) {
                while (true) {
                    String message = input.readLine();
                    if (message == null || "".equals(message.trim())) break;
                    for (String arg: args) {
                        InetSocketAddress addr = new InetSocketAddress(arg, port);
                        p2p.send(message.getBytes(Charset.defaultCharset()), addr);
                    }
                }
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
    }
}
