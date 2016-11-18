package main.java;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.Properties;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Created by shaheens on 17/11/16.
 */
public class DataGenerator {
    private static final Logger LOGGER = LoggerFactory.getLogger(DataGenerator.class);
    static int counter = 0;

    public static void main(String[] args) throws IOException,
            InterruptedException {
        long startTime = System.currentTimeMillis();
        LOGGER.info("Started time ::: ", startTime);

        /**
         *  Declare file variables with default values
         */
        int numberOfFiles = args.length >= 1 ? Integer.parseInt(args[0]) : 2;
        long minSize = args.length >= 2 ? Long.parseLong(args[1]) : 0;
        long maxSize = args.length >= 3 ? Long.parseLong(args[1]) : 0;

        LOGGER.info("Number of files ::: ", numberOfFiles, "   , minimum size ::: "
                , minSize, "   ,maximum size ::: ", maxSize);

        // Read the config file to get the properties
        Properties configProperties = new Properties();
        configProperties.load(getFileFromResource("config.properties"));

        // Read the file from path using Buffered Reader
        BufferedReader bufferedReader = new BufferedReader(
                new InputStreamReader(getFileFromResource(configProperties
                                .getProperty("fileName"))));

        // Skip the first 3 lines of file
        int linesToSkip = Integer.parseInt(configProperties
                .getProperty("linesToSkip"));

        File file = new File("datageneratorfile.log");
        segregateFiles(bufferedReader, file, linesToSkip);

        long noOfIterations;
        if (maxSize == 0 && minSize == 0) {
            noOfIterations = getFileSize(minSize, maxSize)
                    / file.length();
        } else if (maxSize != 0) {
            noOfIterations = maxSize / file.length();
        } else {
            noOfIterations = minSize / file.length();
        }

        ThreadPoolExecutor executor = new ThreadPoolExecutor(5, 8, 5000, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());

        for (int i = 0; i < numberOfFiles; i++) {
            final long totalIterations = noOfIterations;
            executor.execute(new Runnable() {
                public void run() {
                    try {
                        generateData(totalIterations);
                        Runtime.getRuntime().gc();
                    } catch (IOException e) {
                        LOGGER.error("Exception occurred while generating zip files. ", e);
                    }

                }
            });
        }
        executor.shutdown();
        while (!executor.isTerminated()) {
        }
        LOGGER.info("Finished all threads");
        LOGGER.info("Total time taken ::: ",
                (System.currentTimeMillis() - startTime));
    }

    /**
     * This method creates large file and then zips it
     *
     * @param finalNumberOfTimesToRotate
     * @throws IOException
     */
    private static void generateData(long finalNumberOfTimesToRotate) throws IOException {
        byte[] fileSize = new byte[1024 * 1024 * 512];
        String fileName = getFileName();
        File file = new File(fileName + ".log");

        FileGenerator.createFile(file, finalNumberOfTimesToRotate);

        LOGGER.info("Creating zip file for the file  ::: ", fileName);

        FileGenerator.createZipFIle(fileSize, fileName, file);
        LOGGER.info("Successfully created gzipped file  ::: ", fileName);
    }

    /**
     * @param bufferedReader
     * @param linesToSkip
     * @throws IOException This method is used to create a skippedlinefile,that is used
     *                     to calculate samplefile size
     */
    private static void segregateFiles(BufferedReader bufferedReader, File sourceFile, int linesToSkip)
            throws IOException {
        // create skipLine file to calculate the sample file length
        FileWriter skippedFileWriter = new FileWriter(
                new File("skipFile.log").getAbsoluteFile());

        BufferedWriter bufferedWriter = new BufferedWriter(skippedFileWriter);
        for (int i = 0; i < linesToSkip; i++) {
            String skipLine = bufferedReader.readLine();
            bufferedWriter.write(skipLine);
            bufferedWriter.write("\r\n");
        }
        bufferedWriter.close();
        skippedFileWriter.close();

        String line;
        FileWriter sampleFileWriter = new FileWriter(sourceFile.getAbsoluteFile());
        BufferedWriter dataBufferedWriter = new BufferedWriter(
                new FileWriter(sourceFile.getAbsoluteFile()));
        while ((line = bufferedReader.readLine()) != null) {
            sampleFileWriter.write(line);
            dataBufferedWriter.write("\r\n");
        }

        dataBufferedWriter.close();
        sampleFileWriter.close();

    }

    private static InputStream getFileFromResource(String fileName) throws FileNotFoundException {

        InputStream in = ClassLoader.getSystemResourceAsStream(fileName);
        return in;
    }

    private static long getFileSize(long minSIze, long maxSize) {
        long fileSize = 0;
        if (maxSize == 0 && minSIze == 0) {
            fileSize = (1024 * 1024 * 1024) + (1024 * 1024 * 512);
        }
        return fileSize;
    }

    static synchronized String getFileName() {
        counter++;
        LOGGER.info("datageneratorfile" + counter);
        return "datageneratorfile" + counter;
    }

}
