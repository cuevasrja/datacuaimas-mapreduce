package com.datacuaimas.avro;

import java.io.IOException;
import java.net.InetSocketAddress;

import org.apache.avro.ipc.netty.NettyServer;
import org.apache.avro.ipc.netty.NettyTransceiver;
import org.apache.avro.ipc.Server;
import org.apache.avro.ipc.specific.SpecificRequestor;
import org.apache.avro.ipc.specific.SpecificResponder;
import org.apache.avro.util.Utf8;

import example.proto.Mail;
import example.proto.Message;

/**
 * Start a server, attach a client, and send a message.
 */
public class Main {
    public static class MailImpl implements Mail {
        // in this simple example just return details of the message
        public Utf8 send(Message message) {
            System.out.println("Sending message");
            return new Utf8("Sending message to " + message.getTo().toString()
                    + " from " + message.getFrom().toString()
                    + " with body " + message.getBody().toString());
        }
    }

    private static Server server;

    private static void startServer() throws IOException, InterruptedException {
        server = new NettyServer(new SpecificResponder(Mail.class, new MailImpl()), new InetSocketAddress(65111));
        // the server implements the Mail protocol (MailImpl)
    }

    public static void main(String[] args) throws IOException, InterruptedException {
        if (args.length != 3) {
            System.out.println("Usage: <to> <from> <body>");
            System.exit(1);
        }

        System.out.println("Starting server");
        // usually this would be another app, but for simplicity
        startServer();
        System.out.println("Server started");

        NettyTransceiver client = new NettyTransceiver(new InetSocketAddress(65111));
        // client code - attach to the server and send a message
        Mail proxy = (Mail) SpecificRequestor.getClient(Mail.class, client);
        System.out.println("Client built, got proxy");

        // fill in the Message record and send it
        Message message = new Message();
        message.setTo(new Utf8(args[0]));
        message.setFrom(new Utf8(args[1]));
        message.setBody(new Utf8(args[2]));
        System.out.println("Calling proxy.send with message:  " + message.toString());
        System.out.println("Result: " + proxy.send(message));

        // cleanup
        System.out.println("Closing client");
        client.close(true);
        System.out.println("Closing server");
        server.close();
        server.join();
        System.out.println("Exiting (forcing due to Netty non-daemon thread introduced between Avro 1.9.2 and 1.10.0)");
        System.exit(0);
    }
}