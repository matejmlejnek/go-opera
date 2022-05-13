package launcher

import (
	"context"
	"fmt"
	"github.com/Fantom-foundation/go-opera/direct_sync"
	"github.com/Fantom-foundation/lachesis-base/abft"
	"github.com/Fantom-foundation/lachesis-base/common/bigendian"
	"github.com/Fantom-foundation/lachesis-base/inter/idx"
	"github.com/Fantom-foundation/lachesis-base/kvdb"
	"github.com/Fantom-foundation/lachesis-base/kvdb/flushable"
	"os"
	"path"
	"sort"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/accounts"
	"github.com/ethereum/go-ethereum/accounts/keystore"
	"github.com/ethereum/go-ethereum/cmd/utils"
	"github.com/ethereum/go-ethereum/console/prompt"
	"github.com/ethereum/go-ethereum/ethclient"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/node"
	"github.com/ethereum/go-ethereum/params"
	"gopkg.in/urfave/cli.v1"

	evmetrics "github.com/ethereum/go-ethereum/metrics"

	"github.com/Fantom-foundation/go-opera/cmd/opera/launcher/metrics"
	"github.com/Fantom-foundation/go-opera/cmd/opera/launcher/tracing"
	"github.com/Fantom-foundation/go-opera/debug"
	"github.com/Fantom-foundation/go-opera/evmcore"
	"github.com/Fantom-foundation/go-opera/flags"
	"github.com/Fantom-foundation/go-opera/gossip"
	"github.com/Fantom-foundation/go-opera/gossip/emitter"
	"github.com/Fantom-foundation/go-opera/integration"
	"github.com/Fantom-foundation/go-opera/utils/errlock"
	"github.com/Fantom-foundation/go-opera/valkeystore"
	_ "github.com/Fantom-foundation/go-opera/version"
)

const (
	// clientIdentifier to advertise over the network.
	clientIdentifier = "go-opera"
)

var (
	// Git SHA1 commit hash of the release (set via linker flags).
	gitCommit = ""
	gitDate   = ""
	// The app that holds all commands and flags.
	app = flags.NewApp(gitCommit, gitDate, "the go-opera command line interface")

	nodeFlags        []cli.Flag
	testFlags        []cli.Flag
	gpoFlags         []cli.Flag
	accountFlags     []cli.Flag
	performanceFlags []cli.Flag
	networkingFlags  []cli.Flag
	txpoolFlags      []cli.Flag
	operaFlags       []cli.Flag
	legacyRpcFlags   []cli.Flag
	rpcFlags         []cli.Flag
	metricsFlags     []cli.Flag
)

func initFlags() {
	// Flags for testing purpose.
	testFlags = []cli.Flag{
		FakeNetFlag,
	}

	// Flags that configure the node.
	gpoFlags = []cli.Flag{}
	accountFlags = []cli.Flag{
		utils.UnlockedAccountFlag,
		utils.PasswordFileFlag,
		utils.ExternalSignerFlag,
		utils.InsecureUnlockAllowedFlag,
	}
	performanceFlags = []cli.Flag{
		CacheFlag,
	}
	networkingFlags = []cli.Flag{
		utils.BootnodesFlag,
		utils.ListenPortFlag,
		utils.MaxPeersFlag,
		utils.MaxPendingPeersFlag,
		utils.NATFlag,
		utils.NoDiscoverFlag,
		utils.DiscoveryV5Flag,
		utils.NetrestrictFlag,
		utils.NodeKeyFileFlag,
		utils.NodeKeyHexFlag,
	}
	txpoolFlags = []cli.Flag{
		utils.TxPoolLocalsFlag,
		utils.TxPoolNoLocalsFlag,
		utils.TxPoolJournalFlag,
		utils.TxPoolRejournalFlag,
		utils.TxPoolPriceLimitFlag,
		utils.TxPoolPriceBumpFlag,
		utils.TxPoolAccountSlotsFlag,
		utils.TxPoolGlobalSlotsFlag,
		utils.TxPoolAccountQueueFlag,
		utils.TxPoolGlobalQueueFlag,
		utils.TxPoolLifetimeFlag,
	}
	operaFlags = []cli.Flag{
		GenesisFlag,
		utils.IdentityFlag,
		DataDirFlag,
		utils.MinFreeDiskSpaceFlag,
		utils.KeyStoreDirFlag,
		utils.USBFlag,
		utils.SmartCardDaemonPathFlag,
		utils.ExitWhenSyncedFlag,
		utils.LightKDFFlag,
		configFileFlag,
		validatorIDFlag,
		validatorPubkeyFlag,
		validatorPasswordFlag,
		SyncModeFlag,
		DirectSyncFlagClient,
		DirectSyncFlagServer,
	}
	legacyRpcFlags = []cli.Flag{
		utils.NoUSBFlag,
		utils.LegacyRPCEnabledFlag,
		utils.LegacyRPCListenAddrFlag,
		utils.LegacyRPCPortFlag,
		utils.LegacyRPCCORSDomainFlag,
		utils.LegacyRPCVirtualHostsFlag,
		utils.LegacyRPCApiFlag,
	}

	rpcFlags = []cli.Flag{
		utils.HTTPEnabledFlag,
		utils.HTTPListenAddrFlag,
		utils.HTTPPortFlag,
		utils.HTTPCORSDomainFlag,
		utils.HTTPVirtualHostsFlag,
		utils.GraphQLEnabledFlag,
		utils.GraphQLCORSDomainFlag,
		utils.GraphQLVirtualHostsFlag,
		utils.HTTPApiFlag,
		utils.HTTPPathPrefixFlag,
		utils.WSEnabledFlag,
		utils.WSListenAddrFlag,
		utils.WSPortFlag,
		utils.WSApiFlag,
		utils.WSAllowedOriginsFlag,
		utils.WSPathPrefixFlag,
		utils.IPCDisabledFlag,
		utils.IPCPathFlag,
		RPCGlobalGasCapFlag,
		RPCGlobalTxFeeCapFlag,
	}

	metricsFlags = []cli.Flag{
		utils.MetricsEnabledFlag,
		utils.MetricsEnabledExpensiveFlag,
		utils.MetricsHTTPFlag,
		utils.MetricsPortFlag,
		utils.MetricsEnableInfluxDBFlag,
		utils.MetricsInfluxDBEndpointFlag,
		utils.MetricsInfluxDBDatabaseFlag,
		utils.MetricsInfluxDBUsernameFlag,
		utils.MetricsInfluxDBPasswordFlag,
		utils.MetricsInfluxDBTagsFlag,
		utils.MetricsEnableInfluxDBV2Flag,
		utils.MetricsInfluxDBTokenFlag,
		utils.MetricsInfluxDBBucketFlag,
		utils.MetricsInfluxDBOrganizationFlag,
		tracing.EnableFlag,
	}

	nodeFlags = []cli.Flag{}
	nodeFlags = append(nodeFlags, gpoFlags...)
	nodeFlags = append(nodeFlags, accountFlags...)
	nodeFlags = append(nodeFlags, performanceFlags...)
	nodeFlags = append(nodeFlags, networkingFlags...)
	nodeFlags = append(nodeFlags, txpoolFlags...)
	nodeFlags = append(nodeFlags, operaFlags...)
	nodeFlags = append(nodeFlags, legacyRpcFlags...)
}

// init the CLI app.
func init() {
	overrideFlags()
	overrideParams()

	initFlags()

	// App.

	app.Action = lachesisMain
	app.Version = params.VersionWithCommit(gitCommit, gitDate)
	app.HideVersion = true // we have a command to print the version
	app.Commands = []cli.Command{
		// See accountcmd.go:
		accountCommand,
		walletCommand,
		// see validatorcmd.go:
		validatorCommand,
		// See consolecmd.go:
		consoleCommand,
		attachCommand,
		javascriptCommand,
		// See config.go:
		dumpConfigCommand,
		checkConfigCommand,
		// See misccmd.go:
		versionCommand,
		licenseCommand,
		// See chaincmd.go
		importCommand,
		exportCommand,
		checkCommand,
		// See snapshot.go
		snapshotCommand,
		// See fixdirty.go
		fixDirtyCommand,
	}
	sort.Sort(cli.CommandsByName(app.Commands))

	app.Flags = append(app.Flags, testFlags...)
	app.Flags = append(app.Flags, nodeFlags...)
	app.Flags = append(app.Flags, rpcFlags...)
	app.Flags = append(app.Flags, consoleFlags...)
	app.Flags = append(app.Flags, debug.Flags...)
	app.Flags = append(app.Flags, metricsFlags...)

	app.Before = func(ctx *cli.Context) error {
		if err := debug.Setup(ctx); err != nil {
			return err
		}

		// Start metrics export if enabled
		utils.SetupMetrics(ctx)
		// Start system runtime metrics collection
		go evmetrics.CollectProcessMetrics(3 * time.Second)
		return nil
	}

	app.After = func(ctx *cli.Context) error {
		debug.Exit()
		prompt.Stdin.Close() // Resets terminal mode.

		return nil
	}
}

func Launch(args []string) error {
	return app.Run(args)
}

// opera is the main entry point into the system if no special subcommand is ran.
// It creates a default node based on the command line arguments and runs it in
// blocking mode, waiting for it to be shut down.
func lachesisMain(ctx *cli.Context) error {
	if args := ctx.Args(); len(args) > 0 {
		return fmt.Errorf("invalid command: %q", args[0])
	}

	// TODO: tracing flags
	//tracingStop, err := tracing.Start(ctx)
	//if err != nil {
	//	return err
	//}
	//defer tracingStop()

	cfg := makeAllConfigs(ctx)
	genesisPath := getOperaGenesis(ctx)
	node, _, nodeClose := makeNode(ctx, cfg, genesisPath)
	defer nodeClose()
	startNode(ctx, node)
	node.Wait()
	return nil
}

func makeNode(ctx *cli.Context, cfg *config, genesis integration.InputGenesis) (*node.Node, *gossip.Service, func()) {
	// check errlock file
	errlock.SetDefaultDatadir(cfg.Node.DataDir)
	errlock.Check()

	stack := makeConfigNode(ctx, &cfg.Node)

	chaindataDir := path.Join(cfg.Node.DataDir, "chaindata")
	if err := os.MkdirAll(chaindataDir, 0700); err != nil {
		utils.Fatalf("Failed to create chaindata directory: %v", err)
	}

	hostAdress := ctx.GlobalString(DirectSyncFlagClient.Name)
	if hostAdress != "" {
		//direct_sync.TestIterateTroughDb(gdb)
		//os.Exit(0)

		_ = os.RemoveAll(chaindataDir)

		if err := os.MkdirAll(chaindataDir, 0700); err != nil {
			utils.Fatalf("Failed to create chaindata directory: %v", err)
		}

		producer := integration.DBProducer(chaindataDir, cfg.cachescale)

		dbs, err := integration.MakeFlushableProducer(producer)
		if err != nil {
			log.Crit("DB opening error", "flushable producer", err)
		}
		//gdb, cdb := getStores(dbs, cfg)

		gossipDb := gossip.NewStore(dbs, cfg.OperaStore)

		if err != nil {
			log.Crit("DB opening error", "datadir", cfg.Node.DataDir, "err", err)
		}

		log.Info("directsyncclient")
		direct_sync.DownloadDataFromServer(hostAdress, gossipDb)
		fmt.Println("Finished client sync")

		flushId, err := gossipDb.GetMainDb().Get(integration.FlushIDKey)
		if err != nil {
			log.Info("GossipDB flush not found")
			flushId = append([]byte{flushable.CleanPrefix}, bigendian.Uint64ToBytes(uint64(time.Now().UnixNano()))...)
			err = gossipDb.GetMainDb().Put(integration.FlushIDKey, flushId)
			if err != nil {
				log.Crit("Failed to write CleanPrefix to gossip db")
			}
		}

		gossipDb.Close()

		gossipDb, err = makeRawGossipStore(producer, cfg)
		if err != nil {
			utils.Fatalf("Failed to open 'gossip' database: %v", err)
		}

		mustOpenDB := func(producer kvdb.DBProducer, name string) kvdb.DropableStore {
			db, err := producer.OpenDB(name)
			if err != nil {
				utils.Fatalf("Failed to open '%s' database: %v", name, err)
			}
			return db
		}
		gdbEpoch := gossipDb.GetEpoch()
		gdbValidators := gossipDb.GetValidators()
		gossipDb.Close()

		log.Info(fmt.Sprintf("Loaded Epoch: ", gdbEpoch))
		log.Info(fmt.Sprintf("Loaded Validators: ", gdbValidators))

		cMainDb := mustOpenDB(producer, "lachesis")
		//remove lachesis db
		_ = cMainDb.Close()
		cMainDb.Drop()

		log.Info("Recreating lachesis db")
		cMainDb = mustOpenDB(producer, "lachesis")
		cGetEpochDB := func(epoch idx.Epoch) kvdb.DropableStore {
			log.Info("Fetching lachesis-", epoch)
			return mustOpenDB(producer, fmt.Sprintf("lachesis-%d", epoch))
		}

		panics := func(name string) func(error) {
			return func(err error) {
				log.Crit(fmt.Sprintf("%s error", name), "err", err)
			}
		}

		log.Info("Apllying genesis")
		concensusDb := abft.NewStore(cMainDb, cGetEpochDB, panics("Lachesis store"), cfg.LachesisStore)
		err = concensusDb.ApplyGenesis(&abft.Genesis{
			Epoch:      gdbEpoch,
			Validators: gdbValidators,
		})
		if err != nil {
			log.Crit("failed to init consensus database: " + err.Error())
		}

		log.Info("Apllied genesis")

		err = cMainDb.Put(integration.FlushIDKey, flushId)
		if err != nil {
			log.Crit("Failed to write CleanPrefix to lachesis db")
		}
		_ = concensusDb.Close()
		log.Info("Set FlushIDKey")

	}

	engine, dagIndex, gdb, cdb, genesisStore, blockProc := integration.MakeEngine(integration.DBProducer(chaindataDir, cfg.cachescale), genesis, cfg.AppConfigs())
	_ = genesis.Close()
	metrics.SetDataDir(cfg.Node.DataDir)

	valKeystore := valkeystore.NewDefaultFileKeystore(path.Join(getValKeystoreDir(cfg.Node), "validator"))
	valPubkey := cfg.Emitter.Validator.PubKey
	if key := getFakeValidatorKey(ctx); key != nil && cfg.Emitter.Validator.ID != 0 {
		addFakeValidatorKey(ctx, key, valPubkey, valKeystore)
		coinbase := integration.SetAccountKey(stack.AccountManager(), key, "fakepassword")
		log.Info("Unlocked fake validator account", "address", coinbase.Address.Hex())
	}

	// unlock validator key
	if !valPubkey.Empty() {
		err := unlockValidatorKey(ctx, valPubkey, valKeystore)
		if err != nil {
			utils.Fatalf("Failed to unlock validator key: %v", err)
		}
	}
	signer := valkeystore.NewSigner(valKeystore)

	// Create and register a gossip network service.
	newTxPool := func(reader evmcore.StateReader) gossip.TxPool {
		if cfg.TxPool.Journal != "" {
			cfg.TxPool.Journal = stack.ResolvePath(cfg.TxPool.Journal)
		}
		return evmcore.NewTxPool(cfg.TxPool, reader.Config(), reader)
	}
	svc, err := gossip.NewService(stack, cfg.Opera, gdb, blockProc, engine, dagIndex, newTxPool)
	if err != nil {
		utils.Fatalf("Failed to create the service: %v", err)
	}
	if cfg.Emitter.Validator.ID != 0 {
		svc.RegisterEmitter(emitter.NewEmitter(cfg.Emitter, svc.EmitterWorld(signer)))
	}
	err = engine.Bootstrap(svc.GetConsensusCallbacks())
	if err != nil {
		utils.Fatalf("Failed to bootstrap the engine: %v", err)
	}

	stack.RegisterAPIs(svc.APIs())
	stack.RegisterProtocols(svc.Protocols())
	stack.RegisterLifecycle(svc)

	if ctx.GlobalBool(DirectSyncFlagServer.Name) {
		log.Info("directsyncserver")
		direct_sync.InitServer(gdb, path.Join(chaindataDir, "gossip"), stack.Server().PrivateKey)
	}

	return stack, svc, func() {
		_ = stack.Close()
		gdb.Close()
		_ = cdb.Close()
		genesisStore.Close()
	}
}

func makeConfigNode(ctx *cli.Context, cfg *node.Config) *node.Node {
	stack, err := node.New(cfg)
	if err != nil {
		utils.Fatalf("Failed to create the protocol stack: %v", err)
	}

	return stack
}

// startNode boots up the system node and all registered protocols, after which
// it unlocks any requested accounts, and starts the RPC/IPC interfaces.
func startNode(ctx *cli.Context, stack *node.Node) {
	debug.Memsize.Add("node", stack)

	// Start up the node itself
	utils.StartNode(ctx, stack)

	// Unlock any account specifically requested
	unlockAccounts(ctx, stack)

	// Register wallet event handlers to open and auto-derive wallets
	events := make(chan accounts.WalletEvent, 16)
	stack.AccountManager().Subscribe(events)

	// Create a client to interact with local opera node.
	rpcClient, err := stack.Attach()
	if err != nil {
		utils.Fatalf("Failed to attach to self: %v", err)
	}
	ethClient := ethclient.NewClient(rpcClient)
	go func() {
		// Open any wallets already attached
		for _, wallet := range stack.AccountManager().Wallets() {
			if err := wallet.Open(""); err != nil {
				log.Warn("Failed to open wallet", "url", wallet.URL(), "err", err)
			}
		}
		// Listen for wallet event till termination
		for event := range events {
			switch event.Kind {
			case accounts.WalletArrived:
				if err := event.Wallet.Open(""); err != nil {
					log.Warn("New wallet appeared, failed to open", "url", event.Wallet.URL(), "err", err)
				}
			case accounts.WalletOpened:
				status, _ := event.Wallet.Status()
				log.Info("New wallet appeared", "url", event.Wallet.URL(), "status", status)

				var derivationPaths []accounts.DerivationPath
				if event.Wallet.URL().Scheme == "ledger" {
					derivationPaths = append(derivationPaths, accounts.LegacyLedgerBaseDerivationPath)
				}
				derivationPaths = append(derivationPaths, accounts.DefaultBaseDerivationPath)

				event.Wallet.SelfDerive(derivationPaths, ethClient)

			case accounts.WalletDropped:
				log.Info("Old wallet dropped", "url", event.Wallet.URL())
				event.Wallet.Close()
			}
		}
	}()

	// Spawn a standalone goroutine for status synchronization monitoring,
	// close the node when synchronization is complete if user required.
	if ctx.GlobalBool(utils.ExitWhenSyncedFlag.Name) {
		go func() {
			for first := true; ; first = false {
				// Call ftm_syncing until it returns false
				time.Sleep(5 * time.Second)

				var syncing bool
				err := rpcClient.CallContext(context.TODO(), &syncing, "ftm_syncing")
				if err != nil {
					continue
				}
				if !syncing {
					if !first {
						time.Sleep(time.Minute)
					}
					log.Info("Synchronisation completed. Exiting due to exitwhensynced flag.")
					err = stack.Close()
					if err != nil {
						continue
					}
					return
				}
			}
		}()
	}
}

// unlockAccounts unlocks any account specifically requested.
func unlockAccounts(ctx *cli.Context, stack *node.Node) {
	var unlocks []string
	inputs := strings.Split(ctx.GlobalString(utils.UnlockedAccountFlag.Name), ",")
	for _, input := range inputs {
		if trimmed := strings.TrimSpace(input); trimmed != "" {
			unlocks = append(unlocks, trimmed)
		}
	}
	// Short circuit if there is no account to unlock.
	if len(unlocks) == 0 {
		return
	}
	// If insecure account unlocking is not allowed if node's APIs are exposed to external.
	// Print warning log to user and skip unlocking.
	if !stack.Config().InsecureUnlockAllowed && stack.Config().ExtRPCEnabled() {
		utils.Fatalf("Account unlock with HTTP access is forbidden!")
	}
	ks := stack.AccountManager().Backends(keystore.KeyStoreType)[0].(*keystore.KeyStore)
	passwords := utils.MakePasswordList(ctx)
	for i, account := range unlocks {
		unlockAccount(ks, account, i, passwords)
	}
}
