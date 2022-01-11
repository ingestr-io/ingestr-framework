package io.ingestr.framework.store.repository;

import com.amazonaws.auth.*;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.Headers;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.StorageClass;
import com.amazonaws.services.s3.transfer.TransferManager;
import com.amazonaws.services.s3.transfer.TransferManagerBuilder;
import com.amazonaws.services.s3.transfer.Upload;
import io.ingestr.framework.store.model.ReceiveObject;
import io.ingestr.framework.store.model.StoreObject;
import io.ingestr.framework.store.repository.config.S3StoreConfig;
import io.ingestr.framework.store.utils.CompressedIO;
import lombok.extern.slf4j.Slf4j;

@Slf4j
public class S3StoreRepository implements StoreRepository {
    private S3StoreConfig s3StoreConfig;
    private AmazonS3 amazonS3;

    public S3StoreRepository(S3StoreConfig s3StoreConfig, AmazonS3 amazonS3) {
        this.s3StoreConfig = s3StoreConfig;
        this.amazonS3 = amazonS3;
    }

    public static S3StoreRepository newStore(S3StoreConfig s3StoreConfig) {
        AmazonS3ClientBuilder s3b = AmazonS3ClientBuilder.standard();

        if (s3StoreConfig.getRegion().isPresent()) {
            s3b.setRegion(s3StoreConfig.getRegion().get());
        }
        if (s3StoreConfig.getAccessKey().isPresent() && s3StoreConfig.getSecretKey().isPresent()) {
            log.info("Using Provided access Key = {}", s3StoreConfig.getAccessKey().get());
            s3b.withCredentials(
                    new AWSStaticCredentialsProvider(
                            new BasicAWSCredentials(s3StoreConfig.getAccessKey().get(),
                                    s3StoreConfig.getSecretKey().get()))
            );
        } else {
            s3b.withCredentials(DefaultAWSCredentialsProviderChain.getInstance());
        }

        return new S3StoreRepository(s3StoreConfig, s3b.build());
    }

    public ReceiveObject receive(String key) {

        S3Object s3o = this.amazonS3.getObject(
                s3StoreConfig.getBucketName(),
                key
        );

        ReceiveObject ro = new ReceiveObject(
                key,
                s3o.getObjectContent()
        );
        return ro;
    }

    public void store(StoreObject storeObject) {
        TransferManager tm = null;
        try {

            tm = TransferManagerBuilder.standard()
                    .withS3Client(amazonS3)
                    .withMultipartUploadThreshold((long) (50 * 1024 * 1025))
                    .build();

            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setHeader(Headers.STORAGE_CLASS, StorageClass.Standard);

            CompressedIO cio = CompressedIO.compressed(storeObject.getInputStream());


            PutObjectRequest putObjectRequest = new PutObjectRequest(
                    s3StoreConfig.getBucketName(),
                    storeObject.getKey(),
                    cio.stream(), metadata);

            Upload upload = tm.upload(putObjectRequest);

            // Optionally, wait for the upload to finish before continuing.
            upload.waitForCompletion();

            // Log status
        } catch (Exception e) {
            throw new RuntimeException(e.getMessage(), e);
        } finally {
            if (tm != null)
                tm.shutdownNow();
        }
    }
}
