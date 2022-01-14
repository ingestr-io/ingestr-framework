# Ingestr Framework

This is the core Ingestr Framework containing most of the behaviours and share components needed.
This is acts as a shared module for both the Ingestr SE and Ingestr CE versions.

## Documentation

To read the full documentation about the Ingestr framework go to the [Documentation](https://ingestr.io/docs/latest) on
the Ingestr.io website.

### Running integration tests

You can run integration tests by executing `mvn verify`

### Debugging

To debug the plugin, you first need to publish a snapshot to your Maven local:

```shell
$ mvn install
```

__