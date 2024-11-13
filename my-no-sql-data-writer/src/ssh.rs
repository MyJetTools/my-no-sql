pub enum SshCredentials {
    UserAgent,
    Password(String),
    PrivateKey {
        private_key: String,
        passphrase: String,
    },
}
