package com.example;

import java.net.InetSocketAddress;
import java.net.InterfaceAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class Utils {
    public static Map<NetworkInterface, List<String>> getSiteLocalNetworkInterfaces() throws SocketException {
        Map<NetworkInterface, List<String>> nics = new HashMap<>();
        for (NetworkInterface nic: Collections.list(NetworkInterface.getNetworkInterfaces())) {
            for (InterfaceAddress addr: nic.getInterfaceAddresses()) {
                if (addr.getAddress().isSiteLocalAddress()) {
                    if (!nics.containsKey(nic)) nics.put(nic, new ArrayList<>());
                    nics.get(nic).add(addr.getAddress().getHostAddress());
                }
            }
        }
        return nics;
    }

    public static int ushort2int(short value) {
        return (value < 0 ? 1 + Short.MAX_VALUE : 0) + (value & Short.MAX_VALUE);
    }

    public static void printBytes(byte[] data) {
        int i = 0;
        while (i < data.length) {
            int j = 0;
            while (i < data.length && j < 4) {
                System.out.print(String.format(" %02x", data[i]));
                i++;
                j++;
            }
            System.out.println();
        }
    }

    public static InetSocketAddress parseAddress(String value) {
        String[] split = value.split(":");
        if (split.length != 2) return null;
        return new InetSocketAddress(split[0], Integer.valueOf(split[1]));
    }
}
