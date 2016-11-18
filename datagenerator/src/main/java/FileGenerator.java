package main.java;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.FileChannel;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;
import java.util.zip.GZIPOutputStream;

/**
 * Created by shaheens on 17/11/16.
 */
public class FileGenerator {

    public static void fastCopy(final ReadableByteChannel src, final WritableByteChannel dest) throws IOException {
        final ByteBuffer buffer = ByteBuffer.allocateDirect(16 * 1024);

        while(src.read(buffer) != -1) {
            buffer.flip();
            dest.write(buffer);
            buffer.compact();
        }

        buffer.flip();

        while(buffer.hasRemaining()) {
            dest.write(buffer);
        }
    }

    public static void createZipFIle(byte[] fileSize, String fileName, File file) throws IOException {
        GZIPOutputStream gzipOutputStream = new GZIPOutputStream(new FileOutputStream(fileName +".log"+ ".gz"));
        FileInputStream inputStream = new FileInputStream(file);
        int length;
        while ((length = inputStream.read(fileSize)) > 0) {
            gzipOutputStream.write(fileSize, 0, length);
        }
        inputStream.close();
        gzipOutputStream.finish();
        gzipOutputStream.close();

    }

    public static void createFile(File file, long finalNumberOfTimesToRotate) throws IOException {
        File infile = new File("datageneratorfile.log");
        final InputStream input = new FileInputStream(infile);
        final OutputStream output = new FileOutputStream(file);
        final ReadableByteChannel inputChannel = Channels.newChannel(input);
        final WritableByteChannel outputChannel = Channels.newChannel(output);
        fastCopy(inputChannel, outputChannel);

        final FileChannel inFileChannel = new FileInputStream(infile).getChannel();
        for (int j = 0; j < finalNumberOfTimesToRotate / 16; j++) {
            for (int k = 0; k <= 16; k++) {
                //Transfer data from input channel to output channel
                inFileChannel.transferTo(0, inFileChannel.size(), outputChannel);
            }
        }
        inputChannel.close();
        inFileChannel.close();
        outputChannel.close();
    }
}

