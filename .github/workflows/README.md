# GHA workflow

If you are cloning or forking this repo you will need to configure two secrets for Actions to run correctly.

Secrets can be set via Settings -> Secrets -> New repository secret:

`CR_USER`: is your GH username.  It must be lowercase.
`CR_PAT`:  can be created by following the [Creating a personal access token](https://docs.github.com/en/github/authenticating-to-github/creating-a-personal-access-token) documentation.

When you create your token, the only permission you need to select is `write:packages` **Upload packages to github package registry**, all other necessary permissions will be selected by default.

You may also need to enable [Improved container support](https://docs.github.com/en/packages/guides/enabling-improved-container-support) in order to allow the images to be written to your repository.  You'll see an error to this affect if this is the case.

Once you have run the build once with those secrets, you have to make the images public.  Access the packages at https://ghcr.io/USER/indy-plenum/plenum-build and https://ghcr.io/USER/indy-plenum/plenum-lint and change the visibility in 'Package Settings' to 'Public' then re-run the build.  Alternatively, if you would prefer to keep the images private, you can manage access to the package and select only the user account associated with the token you setup above.