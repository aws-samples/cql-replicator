1. ```bash
    aws cloudformation deploy
    --stack-name efs-cfn \
    --template-file ecs.yml
    --parameter-overrides App=${YOUR_APP} Env=${YOUR_ENV} ```

2. To get the EFS filesystem ID, you can run a describe-stacks call
   ```bash
   aws cloudformation describe-stacks --stack-name efs-cfn | \
   jq -r '.Stacks[] | .Outputs[] | .OutputValue'
   ```

3. Then, in the manifest of the service which you would like to have access to the EFS filesystem, add the following configuration.
```yaml
storage:
  volumes:
    copilotVolume: # This is a variable key and can be set to arbitrary strings.
      path: '/etc/mount1'
      read_only: true # Set to false if your service needs write access.
      efs:
        id: <your filesystem ID>
```

4. Finally, run 
   ```bash 
   copilot svc deploy
   ```