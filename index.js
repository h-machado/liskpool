let Q = require("q"),
    axios = require("axios")
    stdio = require("stdio")
    winston = require("winston"),
    fs = require('fs');

const options = stdio.getopt({
  'configuration': {       "key": 'c', "args": 1, "description": "Path to configuration json file, check example in config.sample.json", "default": "./config.json" },
  'logLevel':      {       "key": 'L', "args": 1, "description": "Loglevel", "default": "INFO"},
  'dryRun':        {       "key": 'n', "args": 0, "description": "Dry run, use to test and display distribution without transfers being issued", "default": false }
});

let consoleLogRules = new winston.transports.Console({
  'level': options.logLevel.toLowerCase() || "info",
  'timestamp': function() {
    return (new Date()).toISOString();
  },
  'formatter': function(options) {
    return options.timestamp() +' '+ options.level.toUpperCase() +' '+ (undefined !== options.message ? options.message : '') + (options.meta && Object.keys(options.meta).length ? ''+ JSON.stringify(options.meta) : '' );
  }
});

let logger = new (winston.Logger)({
	transports: [ consoleLogRules ],
	exceptionHandlers: [ consoleLogRules ]
});
logger.info("Starting...");

let config = {}
try {
	config = require(options.configuration);
} catch(e) {
	logger.error(`Configuration file ${configuration} is an invalid json file: ${e.message}`);
	process.exit(15);
}

//pool account information
const poolaccount = config.poolaccount;
//runtime variables contain initialization and runtime values; check initialParametersLoaded promise
let runtime = {};
//parters variables contain all voters for pool members
let partners = {};

const initialParametersLoad = [
  //runtime's members key from the following url:
  `${config.lists.members.url}`,
  //runtime's balance key from the following url:
  `${config.lisknode.url}/api/accounts/getBalance?address=${poolaccount.address}`,
  //runtime's accounts key from the following url:
  `${config.lisknode.url}/api/delegates/voters?publicKey=${poolaccount.publicKey}`
];

const terminalRejectionCatchall = (e) => {
  logger.error("Unknown promise rejection, exiting... (catchall)");
  console.log(e);
  process.exit(101);
}

const terminalFailureCatchall = (e) => {
  logger.error("Unknown failure, exiting... (catchall)");
  console.log(e);
  process.exit(601);
}

//get members' list, voters and balance for pool
let initialParametersLoaded = initialParametersLoad.reduce((promise, url) => {
  return promise.then(() => {
    return axios.get(url)
      .then((response) => {
        if ((response.data.success === true) || (response.data.success === "true")) {
          runtime = Object.assign(runtime, response.data);
        } else {
          const returnCode = 12;
          logger.error(`Unsuccessful response from ${url}; exiting with code ${returnCode}`);
          process.exit(returnCode);
        }
      }, terminalRejectionCatchall)
      .catch((e) => {
        const returnCode = 13;
        logger.error(`Unable to get parameters from ${url} ${e.message}; exiting with code ${returnCode}`);
        process.exit(returnCode);
        //console.log(e)
      });
  });
}, Q.resolve());

//get the members and produce a list to get all their voters, send to in-mem struct
const membersVotersLoad = (bonus) => {
  logger.info(`Going through pool members votes for ${bonus} bonus award`);
  var deferred = Q.defer();
  let votersLoaded = runtime.delegates.reduce((promise, member) => {
    return promise.then(() => {
      var url = `${config.lisknode.url}/api/delegates/voters?publicKey=${member[config.lisknode.environment].publicKey}`;
      logger.debug(`Loading ${member.username} voters`);
      return axios.get(url).then((response) => {
        response.data.accounts.map((account) => {
          const { username, address, publicKey, balance } = account;
          const votes = 1;
          logger.silly(`Username ${username}/${address} voted for ${member.username}`)
          if (partners[address] === undefined) {
            partners[address] = { username, address, publicKey, balance, votes };
          } else {
            partners[address].votes = partners[address].votes + 1;
          }
        }, terminalRejectionCatchall);
      }).catch(terminalFailureCatchall);
    });

  }, Q.resolve());

  votersLoaded.then(() => {
    deferred.resolve()
  });

  return deferred.promise;
}

const awardDryRun = (transfer) => {
  var deferred = Q.defer();
  if (transfer === undefined) {
    deferred.resolve();
  } else {
    let { amount, recipientId, paymentClass } = transfer;
    logger.info(`Awarding ${amount}(${amount / 100000000}LSK) to ${recipientId} - ${paymentClass}`);
    deferred.resolve();
  }
  return deferred.promise;
};

const payout = (distribution) => {
  var deferred = Q.defer();
  let transactionList = {};


  let payoutsReady = distribution.reduce((promise, payload) => {
    return promise.then(() => {
      const url = `${config.lisknode.url}/api/transactions`;
      const { amount, recipientId } = payload;
      const { secret, publicKey, secondSecret } = poolaccount;
      const transfer = (poolaccount.secondSecret === undefined)?
        { amount, recipientId, secret, publicKey }:
        { amount, recipientId, secret, publicKey, secondSecret };
      if (options.dryRun === true) {
        return awardDryRun(payload);
      } else {
        return axios.put(url, transfer).then((response) => {
          if (response.data.success === true) {
            logger.info(`Transfer of ${amount} to ${recipientId} succeeded (txid: ${response.data.transactionId})`);
            //payload["transactionId"] = response.data.transactionId;
          } else {
            logger.error(`Failed to process transfer of ${amount} to ${recipientId} (err: ${response.data.error})`);
          }
          //fs.writeFileSync((new Date()).toISOString().replace(/:/g, ".") + ".distribution.json", JSON.stringify(distribution, null, "  "));
        }, terminalRejectionCatchall);
      }
    }).catch(terminalFailureCatchall);
  }, Q.resolve());

  payoutsReady.then(() => {
    deferred.resolve()
  });

  return deferred.promise;
}

// TODO: Optimize
const isPoolMember = (account) => {
  let poolMember = false;
  runtime.delegates.map((member) => {
    poolMember = poolMember || account.publicKey === member[config.lisknode.environment].publicKey || account.address === member[config.lisknode.environment].address;
  });
  return poolMember;
}

initialParametersLoaded.then(() => {
  //Parameters needed are loaded, compute and distribute payouts
  logger.info("Loaded, proceeding to payouts");
  const totalAmmountForUsers = runtime.accounts.reduce((total, obj) => {
    return total + parseFloat(obj["balance"]);
  }, 0);

  const totalAmmountFromPoolMembers = runtime.accounts.reduce((total, obj) => {
    return isPoolMember(obj)?total + parseFloat(obj["balance"]):total;
  }, 0);

  const poolBonus = Math.floor(totalAmmountFromPoolMembers * poolaccount.distributionPercent * runtime.balance / totalAmmountForUsers);
  const poolAward = Math.floor(runtime.balance * poolaccount.distributionPercent);

  logger.info(`Pool account has ${runtime.balance}`);
  logger.info(`Pool account will distribute ${poolAward}`);
  logger.info(`Total lisk in voting for pool is ${totalAmmountForUsers}`)
  logger.info(`Total lisk from poolMembers voting for pool is ${totalAmmountFromPoolMembers}`)
  logger.info(`Bonus ammount from pool is ${poolBonus}, to be distributed to supporter level members`);

  let totalDistributed = 0;
  const distribution = runtime.accounts.map((account) => {
    const reward = Math.floor(parseInt(account.balance) * parseFloat(poolaccount.distributionPercent) * parseInt(runtime.balance) / totalAmmountForUsers);
    totalDistributed = totalDistributed + reward;
    if (isPoolMember(account)) {
      logger.debug(`Rejecting ${reward}(${reward / 100000000}LSK) to ${account.address || account.username} with weight ${account.balance} because is a pool member`);
      return undefined;
    } else {
      logger.debug(`Preparing ${reward}(${reward / 100000000}LSK) to ${account.address || account.username} with weight ${account.balance} `);
      return {
        "amount" : reward,
        "recipientId" : account.address || account.username,
        "paymentClass": "pool reward"
      }
    }
  });

  logger.info(`Total distributed is ${totalDistributed}`)
  membersVotersLoad(poolBonus).then(() => {
    logger.info("Members voters loaded, computing bonuses");
    let bonusCandidatesSum = 0;
    let bonusDistributionList = [];
    Object.keys(partners).forEach((key) => {
      const partner = partners[key];
      const { username, address, publicKey, balance, votes } = partner;
      if (isPoolMember(partner)) {
        logger.debug(`Username ${username}/${address} is a pool member (${votes}/${runtime.delegates.length}), and there are no bonus payouts for pool members`);
      } else if ((votes === runtime.delegates.length) && (!isPoolMember(partner))) {
        logger.info(`Username ${username}/${address} voted for all members with weight ${partner.balance} and is a bonus candidate`);
        bonusCandidatesSum += parseInt(partner.balance);
        bonusDistributionList.push(partner);
      } else {
        logger.debug(`Username ${username}/${address} voted for ${votes}/${runtime.delegates.length} of members, no bonus payout`);
      };
    });

    bonusDistributionList.map((account) => {
      let crap = { "a": parseInt(account.balance), "b": bonusCandidatesSum, "c": poolBonus };
      account["amount"] = Math.floor(parseInt(account.balance) * poolBonus / bonusCandidatesSum);
      account["recipientId"] = account["address"];
      account["paymentClass"] = "bonus reward";
      distribution.push(account);
    });

    logger.info("Filtering and sorting transfers");
    const transfers = distribution.filter((account) => {
      return ((account !== undefined) && (parseInt(account.amount) - 100000000 > 1));
    });

    fs.writeFileSync((new Date()).toISOString().replace(/:/g, ".") + ".json", JSON.stringify({ runtime, config, poolBonus, poolAward, transfers }, null, "  "));

    logger.info("Performing transfers");
    payout(transfers).then(() => {
      logger.info(`Payouts have executed ${options.dryRun?"(dry-run, no transfers issued)":""}`);
    });
  }).catch(terminalFailureCatchall);
});
