import {
	IExecuteFunctions,
	INodeExecutionData,
	INodeType,
	INodeTypeDescription,
	NodeOperationError,
} from 'n8n-workflow';

import * as https from 'https';

// ============================================================
// LND REST API helper
// ============================================================
async function lndRequest(
	context: IExecuteFunctions,
	method: string,
	endpoint: string,
	body: any,
	restHost: string,
	macaroon: string,
	tlsCert: string | undefined,
): Promise<any> {
	return new Promise((resolve, reject) => {
		const urlClean = restHost.replace(/^https?:\/\//, '');
		const hostname = urlClean.split(':')[0];
		const port = parseInt(urlClean.split(':')[1] || '8080');

		const agentOptions: https.AgentOptions = {};
		if (tlsCert && tlsCert.trim() !== '') {
			agentOptions.ca = Buffer.from(tlsCert, 'utf-8');
		} else {
			agentOptions.rejectUnauthorized = false;
		}

		const options: https.RequestOptions = {
			hostname,
			port,
			path: endpoint,
			method,
			headers: {
				'Grpc-Metadata-macaroon': macaroon,
				'Content-Type': 'application/json',
			},
			agent: new https.Agent(agentOptions),
		};

		const req = https.request(options, (res) => {
			let data = '';
			res.on('data', (chunk) => {
				data += chunk;
			});
			res.on('end', () => {
				try {
					const parsedData = JSON.parse(data);
					if (res.statusCode && res.statusCode >= 200 && res.statusCode < 300) {
						resolve(parsedData);
					} else {
						reject(
							new NodeOperationError(
								context.getNode(),
								`LND API error (${res.statusCode}): ${parsedData.error || parsedData.message || data}`,
							),
						);
					}
				} catch (error) {
					const errorMessage = error instanceof Error ? error.message : 'Unknown parsing error';
					reject(
						new NodeOperationError(
							context.getNode(),
							`Error parsing response: ${errorMessage}`,
						),
					);
				}
			});
		});

		req.on('error', (error) => {
			const errorMessage = error instanceof Error ? error.message : 'Unknown connection error';
			reject(
				new NodeOperationError(
					context.getNode(),
					`Connection error to LND: ${errorMessage}`,
				),
			);
		});

		if (method !== 'GET' && body) {
			req.write(JSON.stringify(body));
		}

		req.end();
	});
}

// ============================================================
// Helper: compute time range
// ============================================================
function getTimeRange(
	context: IExecuteFunctions,
	itemIndex: number,
): { startTime: number; endTime: number } {
	const timeRange = context.getNodeParameter('timeRange', itemIndex) as string;
	const now = Math.floor(Date.now() / 1000);
	let startTime: number;
	let endTime: number = now;

	switch (timeRange) {
		case '1h':
			startTime = now - 3600;
			break;
		case '24h':
			startTime = now - 86400;
			break;
		case '7d':
			startTime = now - 604800;
			break;
		case '30d':
			startTime = now - 2592000;
			break;
		case 'custom':
			startTime = context.getNodeParameter('startTime', itemIndex) as number;
			endTime = context.getNodeParameter('endTime', itemIndex) as number;
			break;
		default:
			startTime = now - 86400;
	}

	return { startTime, endTime };
}

// ============================================================
// Helper: fetch all forwarding events (paginated)
// ============================================================
async function getAllForwardingEvents(
	context: IExecuteFunctions,
	startTime: number,
	endTime: number,
	restHost: string,
	macaroon: string,
	tlsCert: string | undefined,
): Promise<any[]> {
	let allEvents: any[] = [];
	let indexOffset = 0;
	const pageSize = 10000;
	let hasMore = true;

	while (hasMore) {
		const resp = await lndRequest(
			context,
			'POST',
			'/v1/switch',
			{
				start_time: startTime.toString(),
				end_time: endTime.toString(),
				num_max_events: pageSize,
				index_offset: indexOffset,
			},
			restHost,
			macaroon,
			tlsCert,
		);

		const events = resp.forwarding_events || [];
		allEvents = allEvents.concat(events);

		if (events.length < pageSize) {
			hasMore = false;
		} else {
			indexOffset = parseInt(resp.last_offset_index || '0');
		}
	}

	return allEvents;
}

export class LightningNetwork implements INodeType {
	description: INodeTypeDescription = {
		displayName: 'Lightning Network (LND)',
		name: 'lightningNetwork',
		icon: 'file:lightning.svg',
		group: ['transform'],
		version: 2,
		subtitle: '={{$parameter["resource"] + " / " + $parameter["operation"]}}',
		description: 'Advanced Lightning Network Daemon (LND) management - routing, fees, channels, payments & monitoring',
		defaults: {
			name: 'Lightning Network',
		},
		inputs: ['main'],
		outputs: ['main'],
		credentials: [
			{
				name: 'lndApi',
				required: true,
			},
		],
		properties: [
			// ========== RESOURCE ==========
			{
				displayName: 'Resource',
				name: 'resource',
				type: 'options',
				noDataExpression: true,
				options: [
					{
						name: 'Node',
						value: 'node',
						description: 'Node info & wallet',
					},
					{
						name: 'Channel',
						value: 'channel',
						description: 'Channel management',
					},
					{
						name: 'Routing',
						value: 'routing',
						description: 'Forwarding & routing analysis',
					},
					{
						name: 'Fee',
						value: 'fees',
						description: 'Fee policies management',
					},
					{
						name: 'Payment',
						value: 'payment',
						description: 'Send & receive payments',
					},
					{
						name: 'Peer',
						value: 'peer',
						description: 'Peer management & scoring',
					},
					{
						name: 'Rebalance',
						value: 'rebalance',
						description: 'Channel rebalancing',
					},
					{
						name: 'Wallet',
						value: 'wallet',
						description: 'On-chain wallet, UTXO management & dust protection',
					},
				],
				default: 'node',
			},

			// ========== NODE OPERATIONS ==========
			{
				displayName: 'Operation',
				name: 'operation',
				type: 'options',
				noDataExpression: true,
				displayOptions: { show: { resource: ['node'] } },
				options: [
					{
						name: 'Get Info',
						value: 'getInfo',
						description: 'Get node information (version, channels, synced, etc.)',
						action: 'Get node information',
					},
					{
						name: 'Get Balance',
						value: 'getBalance',
						description: 'Get on-chain & channel balances',
						action: 'Get on chain channel balances',
					},
					{
						name: 'Get Network Info',
						value: 'getNetworkInfo',
						description: 'Get Lightning network statistics',
						action: 'Get lightning network statistics',
					},
					{
						name: 'Sign Message',
						value: 'signMessage',
						description: 'Sign a message with the node key',
						action: 'Sign a message with the node key',
					},
					{
						name: 'Verify Message',
						value: 'verifyMessage',
						description: 'Verify a signed message',
						action: 'Verify a signed message',
					},
				],
				default: 'getInfo',
			},

			// ========== CHANNEL OPERATIONS ==========
			{
				displayName: 'Operation',
				name: 'operation',
				type: 'options',
				noDataExpression: true,
				displayOptions: { show: { resource: ['channel'] } },
				options: [
					{
						name: 'List Channels',
						value: 'listChannels',
						description: 'List all channels with balance details',
						action: 'List all channels with balance details',
					},
					{
						name: 'List Pending Channels',
						value: 'listPendingChannels',
						description: 'List pending open/close channels',
						action: 'List pending open close channels',
					},
					{
						name: 'List Closed Channels',
						value: 'listClosedChannels',
						description: 'List closed channels history',
						action: 'List closed channels history',
					},
					{
						name: 'Open Channel',
						value: 'openChannel',
						description: 'Open a new channel with a peer',
						action: 'Open a new channel with a peer',
					},
					{
						name: 'Close Channel',
						value: 'closeChannel',
						description: 'Close a channel (cooperative or force)',
						action: 'Close a channel',
					},
					{
						name: 'Balance Monitor',
						value: 'balanceMonitor',
						description: 'Get channels with balance ratio & imbalance alerts',
						action: 'Get channels with balance ratio imbalance alerts',
					},
					{
						name: 'HTLC Monitor',
						value: 'htlcMonitor',
						description: 'Monitor pending HTLCs per channel - detect dust attacks',
						action: 'Monitor pending HTLCs per channel detect dust attacks',
					},
					{
						name: 'Update Min HTLC',
						value: 'updateMinHtlc',
						description: 'Set minimum HTLC size per channel to block dust HTLCs',
						action: 'Set minimum HTLC size per channel to block dust HTLCs',
					},
				],
				default: 'listChannels',
			},

			// ========== ROUTING OPERATIONS ==========
			{
				displayName: 'Operation',
				name: 'operation',
				type: 'options',
				noDataExpression: true,
				displayOptions: { show: { resource: ['routing'] } },
				options: [
					{
						name: 'Forwarding History',
						value: 'forwardingHistory',
						description: 'Get forwarding events with revenue details',
						action: 'Get forwarding events with revenue details',
					},
					{
						name: 'Forwarding Summary',
						value: 'forwardingSummary',
						description: 'Aggregated forwarding stats per channel with revenue',
						action: 'Aggregated forwarding stats per channel with revenue',
					},
					{
						name: 'Query Routes',
						value: 'queryRoutes',
						description: 'Find a route to a destination node',
						action: 'Find a route to a destination node',
					},
					{
						name: 'Dust Analysis',
						value: 'dustAnalysis',
						description: 'Detect dust attack patterns - suspicious micro-forwards',
						action: 'Detect dust attack patterns suspicious micro forwards',
					},
				],
				default: 'forwardingHistory',
			},

			// ========== FEES OPERATIONS ==========
			{
				displayName: 'Operation',
				name: 'operation',
				type: 'options',
				noDataExpression: true,
				displayOptions: { show: { resource: ['fees'] } },
				options: [
					{
						name: 'Get Fee Report',
						value: 'feeReport',
						description: 'Get current fee policies for all channels',
						action: 'Get current fee policies for all channels',
					},
					{
						name: 'Update Channel Fee',
						value: 'updateFee',
						description: 'Update fee policy for a specific channel or all channels',
						action: 'Update fee policy for a specific channel or all channels',
					},
					{
						name: 'Auto Fee Suggestion',
						value: 'autoFeeSuggestion',
						description: 'Suggest optimal fees based on channel balance ratios',
						action: 'Suggest optimal fees based on channel balance ratios',
					},
				],
				default: 'feeReport',
			},

			// ========== PAYMENT OPERATIONS ==========
			{
				displayName: 'Operation',
				name: 'operation',
				type: 'options',
				noDataExpression: true,
				displayOptions: { show: { resource: ['payment'] } },
				options: [
					{
						name: 'Create Invoice',
						value: 'createInvoice',
						description: 'Create a Lightning invoice',
						action: 'Create a lightning invoice',
					},
					{
						name: 'Send Payment',
						value: 'sendPayment',
						description: 'Pay a Lightning invoice (BOLT11)',
						action: 'Pay a lightning invoice',
					},
					{
						name: 'List Invoices',
						value: 'listInvoices',
						description: 'List received invoices',
						action: 'List received invoices',
					},
					{
						name: 'List Payments',
						value: 'listPayments',
						description: 'List outgoing payments',
						action: 'List outgoing payments',
					},
					{
						name: 'Decode Invoice',
						value: 'decodeInvoice',
						description: 'Decode a BOLT11 payment request',
						action: 'Decode a BOLT11 payment request',
					},
				],
				default: 'createInvoice',
			},

			// ========== PEER OPERATIONS ==========
			{
				displayName: 'Operation',
				name: 'operation',
				type: 'options',
				noDataExpression: true,
				displayOptions: { show: { resource: ['peer'] } },
				options: [
					{
						name: 'List Peers',
						value: 'listPeers',
						description: 'List connected peers',
						action: 'List connected peers',
					},
					{
						name: 'Connect Peer',
						value: 'connectPeer',
						description: 'Connect to a new peer',
						action: 'Connect to a new peer',
					},
					{
						name: 'Disconnect Peer',
						value: 'disconnectPeer',
						description: 'Disconnect a peer',
						action: 'Disconnect a peer',
					},
					{
						name: 'Peer Scoring',
						value: 'peerScoring',
						description: 'Score peers by routing revenue, uptime & reliability',
						action: 'Score peers by routing revenue uptime reliability',
					},
					{
						name: 'Get Node Info',
						value: 'getNodeInfo',
						description: 'Get public info about a remote node',
						action: 'Get public info about a remote node',
					},
				],
				default: 'listPeers',
			},

			// ========== REBALANCE OPERATIONS ==========
			{
				displayName: 'Operation',
				name: 'operation',
				type: 'options',
				noDataExpression: true,
				displayOptions: { show: { resource: ['rebalance'] } },
				options: [
					{
						name: 'Circular Rebalance',
						value: 'circularRebalance',
						description: 'Rebalance via circular payment (send to self)',
						action: 'Rebalance via circular payment',
					},
					{
						name: 'Suggest Rebalances',
						value: 'suggestRebalances',
						description: 'Suggest channels that need rebalancing',
						action: 'Suggest channels that need rebalancing',
					},
				],
				default: 'suggestRebalances',
			},

			// ========== WALLET OPERATIONS ==========
			{
				displayName: 'Operation',
				name: 'operation',
				type: 'options',
				noDataExpression: true,
				displayOptions: { show: { resource: ['wallet'] } },
				options: [
					{
						name: 'List UTXOs',
						value: 'listUtxos',
						description: 'List all unspent transaction outputs',
						action: 'List all unspent transaction outputs',
					},
					{
						name: 'Detect Dust UTXOs',
						value: 'detectDustUtxos',
						description: 'Detect dust UTXOs that may be from dust attacks',
						action: 'Detect dust UTXOs that may be from dust attacks',
					},
					{
						name: 'Freeze UTXO (Lease)',
						value: 'leaseOutput',
						description: 'Freeze a UTXO to prevent it from being spent',
						action: 'Freeze a UTXO to prevent it from being spent',
					},
					{
						name: 'Unfreeze UTXO (Release)',
						value: 'releaseOutput',
						description: 'Unfreeze a previously frozen UTXO',
						action: 'Unfreeze a previously frozen UTXO',
					},
				],
				default: 'listUtxos',
			},

			// =============================================
			// PARAMETERS
			// =============================================

			// --- Node: Sign Message ---
			{
				displayName: 'Message',
				name: 'signMsg',
				type: 'string',
				default: '',
				required: true,
				displayOptions: { show: { resource: ['node'], operation: ['signMessage'] } },
				description: 'The message to sign with the node key',
			},

			// --- Node: Verify Message ---
			{
				displayName: 'Message',
				name: 'verifyMsg',
				type: 'string',
				default: '',
				required: true,
				displayOptions: { show: { resource: ['node'], operation: ['verifyMessage'] } },
				description: 'The message that was signed',
			},
			{
				displayName: 'Signature',
				name: 'verifySignature',
				type: 'string',
				default: '',
				required: true,
				displayOptions: { show: { resource: ['node'], operation: ['verifyMessage'] } },
				description: 'The signature to verify',
			},

			// --- Channel: Open Channel ---
			{
				displayName: 'Node Pubkey',
				name: 'nodePubkey',
				type: 'string',
				default: '',
				required: true,
				displayOptions: { show: { resource: ['channel'], operation: ['openChannel'] } },
				description: 'Public key of the node to open a channel with',
			},
			{
				displayName: 'Local Funding Amount (Sats)',
				name: 'localFundingAmount',
				type: 'number',
				default: 100000,
				required: true,
				displayOptions: { show: { resource: ['channel'], operation: ['openChannel'] } },
				description: 'Amount in satoshis to commit to the channel',
			},
			{
				displayName: 'Push Amount (Sats)',
				name: 'pushSat',
				type: 'number',
				default: 0,
				displayOptions: { show: { resource: ['channel'], operation: ['openChannel'] } },
				description: 'Amount to push to the remote side on open',
			},
			{
				displayName: 'Target Confirmations',
				name: 'targetConf',
				type: 'number',
				default: 3,
				displayOptions: { show: { resource: ['channel'], operation: ['openChannel'] } },
				description: 'Target number of blocks for the funding transaction',
			},
			{
				displayName: 'Sat Per Vbyte',
				name: 'satPerVbyte',
				type: 'number',
				default: 0,
				displayOptions: { show: { resource: ['channel'], operation: ['openChannel'] } },
				description: 'Fee rate in sat/vbyte (0 = use target conf)',
			},
			{
				displayName: 'Private Channel',
				name: 'privateChannel',
				type: 'boolean',
				default: false,
				displayOptions: { show: { resource: ['channel'], operation: ['openChannel'] } },
				description: 'Whether the channel should be private (not announced)',
			},

			// --- Channel: Close Channel ---
			{
				displayName: 'Channel Point (txid:output_index)',
				name: 'channelPoint',
				type: 'string',
				default: '',
				required: true,
				displayOptions: { show: { resource: ['channel'], operation: ['closeChannel'] } },
				description: 'The channel point in format txid:output_index',
			},
			{
				displayName: 'Force Close',
				name: 'force',
				type: 'boolean',
				default: false,
				displayOptions: { show: { resource: ['channel'], operation: ['closeChannel'] } },
				description: 'Whether to force close the channel unilaterally',
			},
			{
				displayName: 'Sat Per Vbyte',
				name: 'closeSatPerVbyte',
				type: 'number',
				default: 0,
				displayOptions: { show: { resource: ['channel'], operation: ['closeChannel'] } },
				description: 'Fee rate for closing tx (0 = auto)',
			},

			// --- Channel: Balance Monitor ---
			{
				displayName: 'Imbalance Threshold (%)',
				name: 'imbalanceThreshold',
				type: 'number',
				default: 20,
				displayOptions: { show: { resource: ['channel'], operation: ['balanceMonitor'] } },
				description: 'Alert if local balance ratio is below this % or above (100 - this %)',
			},

			// --- Channel: HTLC Monitor ---
			{
				displayName: 'HTLC Warning Threshold',
				name: 'htlcWarningThreshold',
				type: 'number',
				default: 200,
				displayOptions: { show: { resource: ['channel'], operation: ['htlcMonitor'] } },
				description: 'Warn if pending HTLC count exceeds this value (max per spec: 483)',
			},
			{
				displayName: 'Dust Threshold (Sats)',
				name: 'dustThresholdSat',
				type: 'number',
				default: 546,
				displayOptions: { show: { resource: ['channel'], operation: ['htlcMonitor'] } },
				description: 'Consider HTLCs below this amount as dust (default: 546 sats = Bitcoin dust limit)',
			},

			// --- Channel: Update Min HTLC ---
			{
				displayName: 'Channel Point',
				name: 'minHtlcChannelPoint',
				type: 'string',
				default: '',
				displayOptions: { show: { resource: ['channel'], operation: ['updateMinHtlc'] } },
				description: 'Channel point (txid:output_index). Leave empty to update ALL channels.',
			},
			{
				displayName: 'Min HTLC (Msat)',
				name: 'minHtlcMsat',
				type: 'number',
				default: 1000,
				required: true,
				displayOptions: { show: { resource: ['channel'], operation: ['updateMinHtlc'] } },
				description: 'Minimum HTLC size in millisatoshis (1000 msat = 1 sat)',
			},

			// --- Routing: Dust Analysis ---
			{
				displayName: 'Time Range',
				name: 'dustTimeRange',
				type: 'options',
				options: [
					{ name: 'Last Hour', value: '1h' },
					{ name: 'Last 24 Hours', value: '24h' },
					{ name: 'Last 7 Days', value: '7d' },
					{ name: 'Last 30 Days', value: '30d' },
				],
				default: '24h',
				displayOptions: { show: { resource: ['routing'], operation: ['dustAnalysis'] } },
				description: 'Time period to analyze for dust patterns',
			},
			{
				displayName: 'Dust Threshold (Sats)',
				name: 'dustAnalysisThresholdSat',
				type: 'number',
				default: 100,
				displayOptions: { show: { resource: ['routing'], operation: ['dustAnalysis'] } },
				description: 'Consider forwards below this amount (sats) as potential dust',
			},
			{
				displayName: 'Suspicious Rate Threshold',
				name: 'suspiciousRateThreshold',
				type: 'number',
				default: 50,
				displayOptions: { show: { resource: ['routing'], operation: ['dustAnalysis'] } },
				description: 'Flag a channel if dust forwards exceed this count in the period',
			},

			// --- Routing: Time Range ---
			{
				displayName: 'Time Range',
				name: 'timeRange',
				type: 'options',
				options: [
					{ name: 'Last Hour', value: '1h' },
					{ name: 'Last 24 Hours', value: '24h' },
					{ name: 'Last 7 Days', value: '7d' },
					{ name: 'Last 30 Days', value: '30d' },
					{ name: 'Custom', value: 'custom' },
				],
				default: '24h',
				displayOptions: {
					show: {
						resource: ['routing'],
						operation: ['forwardingHistory', 'forwardingSummary'],
					},
				},
				description: 'Time period for forwarding events',
			},
			{
				displayName: 'Start Time (Unix)',
				name: 'startTime',
				type: 'number',
				default: 0,
				displayOptions: {
					show: {
						resource: ['routing'],
						operation: ['forwardingHistory', 'forwardingSummary'],
						timeRange: ['custom'],
					},
				},
				description: 'Start time as Unix timestamp',
			},
			{
				displayName: 'End Time (Unix)',
				name: 'endTime',
				type: 'number',
				default: 0,
				displayOptions: {
					show: {
						resource: ['routing'],
						operation: ['forwardingHistory', 'forwardingSummary'],
						timeRange: ['custom'],
					},
				},
				description: 'End time as Unix timestamp',
			},

			// --- Routing: Query Routes ---
			{
				displayName: 'Destination Pubkey',
				name: 'destPubkey',
				type: 'string',
				default: '',
				required: true,
				displayOptions: { show: { resource: ['routing'], operation: ['queryRoutes'] } },
				description: 'Public key of the destination node',
			},
			{
				displayName: 'Amount (Sats)',
				name: 'routeAmtSat',
				type: 'number',
				default: 1000,
				required: true,
				displayOptions: { show: { resource: ['routing'], operation: ['queryRoutes'] } },
				description: 'Amount in satoshis to route',
			},

			// --- Fees: Update Fee ---
			{
				displayName: 'Channel Point',
				name: 'feeChannelPoint',
				type: 'string',
				default: '',
				displayOptions: { show: { resource: ['fees'], operation: ['updateFee'] } },
				description: 'Channel point (txid:output_index). Leave empty to update ALL channels (global).',
			},
			{
				displayName: 'Base Fee (Msat)',
				name: 'baseFee',
				type: 'number',
				default: 1000,
				displayOptions: { show: { resource: ['fees'], operation: ['updateFee'] } },
				description: 'Base fee in millisatoshis',
			},
			{
				displayName: 'Fee Rate (Ppm)',
				name: 'feeRate',
				type: 'number',
				default: 100,
				displayOptions: { show: { resource: ['fees'], operation: ['updateFee'] } },
				description: 'Fee rate in parts per million (1 ppm = 0.0001%)',
			},
			{
				displayName: 'Time Lock Delta',
				name: 'timeLockDelta',
				type: 'number',
				default: 40,
				displayOptions: { show: { resource: ['fees'], operation: ['updateFee'] } },
				description: 'The CLTV delta for this channel',
			},

			// --- Fees: Auto Fee Suggestion ---
			{
				displayName: 'Low Balance Fee (Ppm)',
				name: 'lowBalanceFee',
				type: 'number',
				default: 500,
				displayOptions: { show: { resource: ['fees'], operation: ['autoFeeSuggestion'] } },
				description: 'Fee for channels with low local balance (<30%) - discourage drain',
			},
			{
				displayName: 'Balanced Fee (Ppm)',
				name: 'balancedFee',
				type: 'number',
				default: 100,
				displayOptions: { show: { resource: ['fees'], operation: ['autoFeeSuggestion'] } },
				description: 'Fee for well-balanced channels (30-70%)',
			},
			{
				displayName: 'High Balance Fee (Ppm)',
				name: 'highBalanceFee',
				type: 'number',
				default: 10,
				displayOptions: { show: { resource: ['fees'], operation: ['autoFeeSuggestion'] } },
				description: 'Fee for channels with high local balance (>70%) - encourage outflow',
			},
			{
				displayName: 'Base Fee (Msat)',
				name: 'autoBaseFee',
				type: 'number',
				default: 1000,
				displayOptions: { show: { resource: ['fees'], operation: ['autoFeeSuggestion'] } },
				description: 'Base fee to apply in millisatoshis',
			},
			{
				displayName: 'Apply Suggestions',
				name: 'applySuggestions',
				type: 'boolean',
				default: false,
				displayOptions: { show: { resource: ['fees'], operation: ['autoFeeSuggestion'] } },
				description: 'Whether to automatically apply the suggested fees',
			},

			// --- Payment: Create Invoice ---
			{
				displayName: 'Amount (Satoshis)',
				name: 'amount',
				type: 'number',
				default: 1000,
				required: true,
				displayOptions: { show: { resource: ['payment'], operation: ['createInvoice'] } },
				description: 'Invoice amount in satoshis',
			},
			{
				displayName: 'Memo',
				name: 'memo',
				type: 'string',
				default: '',
				displayOptions: { show: { resource: ['payment'], operation: ['createInvoice'] } },
				description: 'Invoice description',
			},
			{
				displayName: 'Expiry (Seconds)',
				name: 'expiry',
				type: 'number',
				default: 3600,
				displayOptions: { show: { resource: ['payment'], operation: ['createInvoice'] } },
				description: 'Invoice validity duration in seconds',
			},

			// --- Payment: Send Payment ---
			{
				displayName: 'Payment Request',
				name: 'paymentRequest',
				type: 'string',
				default: '',
				required: true,
				displayOptions: { show: { resource: ['payment'], operation: ['sendPayment'] } },
				description: 'The Lightning invoice to pay (BOLT11)',
			},
			{
				displayName: 'Timeout (Seconds)',
				name: 'timeout',
				type: 'number',
				default: 60,
				displayOptions: { show: { resource: ['payment'], operation: ['sendPayment'] } },
				description: 'Maximum time for payment completion',
			},
			{
				displayName: 'Fee Limit (Sats)',
				name: 'feeLimitSat',
				type: 'number',
				default: 100,
				displayOptions: { show: { resource: ['payment'], operation: ['sendPayment'] } },
				description: 'Maximum fee in satoshis to pay for routing',
			},

			// --- Payment: Decode Invoice ---
			{
				displayName: 'Payment Request (BOLT11)',
				name: 'decodePayReq',
				type: 'string',
				default: '',
				required: true,
				displayOptions: { show: { resource: ['payment'], operation: ['decodeInvoice'] } },
				description: 'The BOLT11 payment request to decode',
			},

			// --- Peer: Connect ---
			{
				displayName: 'Peer Address (pubkey@host:port)',
				name: 'peerAddress',
				type: 'string',
				default: '',
				required: true,
				displayOptions: { show: { resource: ['peer'], operation: ['connectPeer'] } },
				description: 'Peer address in format pubkey@host:port',
			},

			// --- Peer: Disconnect ---
			{
				displayName: 'Peer Pubkey',
				name: 'disconnectPubkey',
				type: 'string',
				default: '',
				required: true,
				displayOptions: { show: { resource: ['peer'], operation: ['disconnectPeer'] } },
				description: 'Public key of the peer to disconnect',
			},

			// --- Peer: Get Node Info ---
			{
				displayName: 'Node Pubkey',
				name: 'nodeInfoPubkey',
				type: 'string',
				default: '',
				required: true,
				displayOptions: { show: { resource: ['peer'], operation: ['getNodeInfo'] } },
				description: 'Public key of the node to look up',
			},

			// --- Rebalance: Circular ---
			{
				displayName: 'Outgoing Channel ID',
				name: 'outgoingChanId',
				type: 'string',
				default: '',
				required: true,
				displayOptions: { show: { resource: ['rebalance'], operation: ['circularRebalance'] } },
				description: 'Channel ID to send from (high local balance)',
			},
			{
				displayName: 'Last Hop Pubkey',
				name: 'lastHopPubkey',
				type: 'string',
				default: '',
				required: true,
				displayOptions: { show: { resource: ['rebalance'], operation: ['circularRebalance'] } },
				description: 'Pubkey of the peer for the incoming channel (low local balance)',
			},
			{
				displayName: 'Amount (Sats)',
				name: 'rebalanceAmount',
				type: 'number',
				default: 50000,
				required: true,
				displayOptions: { show: { resource: ['rebalance'], operation: ['circularRebalance'] } },
				description: 'Amount to rebalance in satoshis',
			},
			{
				displayName: 'Max Fee (Sats)',
				name: 'rebalanceMaxFee',
				type: 'number',
				default: 50,
				displayOptions: { show: { resource: ['rebalance'], operation: ['circularRebalance'] } },
				description: 'Maximum fee to pay for the rebalance in satoshis',
			},

			// --- Rebalance: Suggest ---
			{
				displayName: 'Target Balance Ratio (%)',
				name: 'targetRatio',
				type: 'number',
				default: 50,
				displayOptions: { show: { resource: ['rebalance'], operation: ['suggestRebalances'] } },
				description: 'Target local balance percentage',
			},
			{
				displayName: 'Min Deviation (%)',
				name: 'minDeviation',
				type: 'number',
				default: 20,
				displayOptions: { show: { resource: ['rebalance'], operation: ['suggestRebalances'] } },
				description: 'Minimum deviation from target to suggest rebalance',
			},

			// --- Wallet: Detect Dust UTXOs ---
			{
				displayName: 'Dust Threshold (Sats)',
				name: 'utxoDustThreshold',
				type: 'number',
				default: 1000,
				displayOptions: { show: { resource: ['wallet'], operation: ['detectDustUtxos'] } },
				description: 'UTXOs below this amount (sats) are considered dust',
			},
			{
				displayName: 'Auto-Freeze Dust',
				name: 'autoFreezeDust',
				type: 'boolean',
				default: false,
				displayOptions: { show: { resource: ['wallet'], operation: ['detectDustUtxos'] } },
				description: 'Automatically lease (freeze) detected dust UTXOs',
			},
			{
				displayName: 'Freeze Duration (Seconds)',
				name: 'freezeDuration',
				type: 'number',
				default: 2592000,
				displayOptions: { show: { resource: ['wallet'], operation: ['detectDustUtxos'], autoFreezeDust: [true] } },
				description: 'Duration to freeze dust UTXOs (default: 30 days)',
			},

			// --- Wallet: Lease Output (Freeze) ---
			{
				displayName: 'Outpoint (txid:index)',
				name: 'leaseOutpoint',
				type: 'string',
				default: '',
				required: true,
				displayOptions: { show: { resource: ['wallet'], operation: ['leaseOutput'] } },
				description: 'UTXO to freeze in format txid:output_index',
			},
			{
				displayName: 'Lease Duration (Seconds)',
				name: 'leaseDuration',
				type: 'number',
				default: 2592000,
				displayOptions: { show: { resource: ['wallet'], operation: ['leaseOutput'] } },
				description: 'How long to freeze this UTXO (default: 30 days = 2592000s)',
			},

			// --- Wallet: Release Output (Unfreeze) ---
			{
				displayName: 'Outpoint (txid:index)',
				name: 'releaseOutpoint',
				type: 'string',
				default: '',
				required: true,
				displayOptions: { show: { resource: ['wallet'], operation: ['releaseOutput'] } },
				description: 'UTXO to unfreeze in format txid:output_index',
			},
		],
	};

	async execute(this: IExecuteFunctions): Promise<INodeExecutionData[][]> {
		const items = this.getInputData();
		const returnData: INodeExecutionData[] = [];

		const credentials = await this.getCredentials('lndApi');
		const restHost = credentials.restHost as string;
		const macaroon = credentials.macaroon as string;
		const tlsCert = credentials.tlsCert as string | undefined;

		for (let i = 0; i < items.length; i++) {
			try {
				let responseData: any;

				const resource = this.getNodeParameter('resource', i) as string;
				const operation = this.getNodeParameter('operation', i) as string;

				// ============ NODE ============
				if (resource === 'node') {
					if (operation === 'getInfo') {
						responseData = await lndRequest(this, 'GET', '/v1/getinfo', null, restHost, macaroon, tlsCert);
					} else if (operation === 'getBalance') {
						const onChain = await lndRequest(this, 'GET', '/v1/balance/blockchain', null, restHost, macaroon, tlsCert);
						const channels = await lndRequest(this, 'GET', '/v1/balance/channels', null, restHost, macaroon, tlsCert);
						responseData = {
							on_chain: onChain,
							channels: channels,
							total_balance_sat:
								parseInt(onChain.total_balance || '0') +
								parseInt(channels.balance || '0'),
						};
					} else if (operation === 'getNetworkInfo') {
						responseData = await lndRequest(this, 'GET', '/v1/graph/info', null, restHost, macaroon, tlsCert);
					} else if (operation === 'signMessage') {
						const msg = this.getNodeParameter('signMsg', i) as string;
						const msgBase64 = Buffer.from(msg, 'utf-8').toString('base64');
						responseData = await lndRequest(
							this, 'POST', '/v1/signmessage',
							{ msg: msgBase64 },
							restHost, macaroon, tlsCert,
						);
					} else if (operation === 'verifyMessage') {
						const msg = this.getNodeParameter('verifyMsg', i) as string;
						const signature = this.getNodeParameter('verifySignature', i) as string;
						const msgBase64 = Buffer.from(msg, 'utf-8').toString('base64');
						responseData = await lndRequest(
							this, 'POST', '/v1/verifymessage',
							{ msg: msgBase64, signature },
							restHost, macaroon, tlsCert,
						);
					}
				}

				// ============ CHANNEL ============
				else if (resource === 'channel') {
					if (operation === 'listChannels') {
						const channelsResp = await lndRequest(this, 'GET', '/v1/channels', null, restHost, macaroon, tlsCert);
						const chans = channelsResp.channels || [];
						responseData = {
							total_channels: chans.length,
							total_capacity_sat: chans.reduce((s: number, c: any) => s + parseInt(c.capacity || '0'), 0),
							total_local_balance_sat: chans.reduce((s: number, c: any) => s + parseInt(c.local_balance || '0'), 0),
							total_remote_balance_sat: chans.reduce((s: number, c: any) => s + parseInt(c.remote_balance || '0'), 0),
							channels: chans.map((c: any) => {
								const cap = parseInt(c.capacity || '0');
								const local = parseInt(c.local_balance || '0');
								const remote = parseInt(c.remote_balance || '0');
								const ratio = cap > 0 ? Math.round((local / cap) * 100) : 0;
								return {
									chan_id: c.chan_id,
									channel_point: c.channel_point,
									remote_pubkey: c.remote_pubkey,
									capacity_sat: cap,
									local_balance_sat: local,
									remote_balance_sat: remote,
									local_ratio_pct: ratio,
									active: c.active,
									private: c.private,
									initiator: c.initiator,
									uptime: c.uptime,
									lifetime: c.lifetime,
									uptime_pct: c.lifetime && parseInt(c.lifetime) > 0
										? Math.round((parseInt(c.uptime || '0') / parseInt(c.lifetime)) * 100)
										: 0,
									total_satoshis_sent: c.total_satoshis_sent,
									total_satoshis_received: c.total_satoshis_received,
									num_updates: c.num_updates,
								};
							}),
						};
					} else if (operation === 'listPendingChannels') {
						responseData = await lndRequest(this, 'GET', '/v1/channels/pending', null, restHost, macaroon, tlsCert);
					} else if (operation === 'listClosedChannels') {
						responseData = await lndRequest(this, 'GET', '/v1/channels/closed', null, restHost, macaroon, tlsCert);
					} else if (operation === 'openChannel') {
						const pubkey = this.getNodeParameter('nodePubkey', i) as string;
						const localAmt = this.getNodeParameter('localFundingAmount', i) as number;
						const pushAmt = this.getNodeParameter('pushSat', i) as number;
						const targetConf = this.getNodeParameter('targetConf', i) as number;
						const satPerVbyte = this.getNodeParameter('satPerVbyte', i) as number;
						const priv = this.getNodeParameter('privateChannel', i) as boolean;

						const body: any = {
							node_pubkey_string: pubkey,
							local_funding_amount: localAmt.toString(),
							push_sat: pushAmt.toString(),
							private: priv,
						};
						if (satPerVbyte > 0) {
							body.sat_per_vbyte = satPerVbyte.toString();
						} else {
							body.target_conf = targetConf;
						}
						responseData = await lndRequest(this, 'POST', '/v1/channels', body, restHost, macaroon, tlsCert);
					} else if (operation === 'closeChannel') {
						const cp = this.getNodeParameter('channelPoint', i) as string;
						const forceClose = this.getNodeParameter('force', i) as boolean;
						const closeFee = this.getNodeParameter('closeSatPerVbyte', i) as number;
						const [fundingTxid, outputIndex] = cp.split(':');
						let endpoint = `/v1/channels/${fundingTxid}/${outputIndex}?force=${forceClose}`;
						if (closeFee > 0) {
							endpoint += `&sat_per_vbyte=${closeFee}`;
						}
						responseData = await lndRequest(this, 'DELETE', endpoint, null, restHost, macaroon, tlsCert);
					} else if (operation === 'balanceMonitor') {
						const threshold = this.getNodeParameter('imbalanceThreshold', i) as number;
						const channelsResp = await lndRequest(this, 'GET', '/v1/channels', null, restHost, macaroon, tlsCert);
						const chans = channelsResp.channels || [];

						const analyzed = chans.map((c: any) => {
							const cap = parseInt(c.capacity || '0');
							const local = parseInt(c.local_balance || '0');
							const ratio = cap > 0 ? Math.round((local / cap) * 100) : 0;
							let status = 'balanced';
							if (ratio < threshold) status = 'depleted_local';
							else if (ratio > (100 - threshold)) status = 'depleted_remote';

							return {
								chan_id: c.chan_id,
								channel_point: c.channel_point,
								remote_pubkey: c.remote_pubkey,
								capacity_sat: cap,
								local_balance_sat: local,
								remote_balance_sat: parseInt(c.remote_balance || '0'),
								local_ratio_pct: ratio,
								status,
								active: c.active,
								needs_rebalance: status !== 'balanced',
							};
						});

						const needsAttention = analyzed.filter((c: any) => c.needs_rebalance);

						responseData = {
							total_channels: analyzed.length,
							balanced_channels: analyzed.length - needsAttention.length,
							imbalanced_channels: needsAttention.length,
							threshold_pct: threshold,
							channels: analyzed,
							alerts: needsAttention,
						};
					} else if (operation === 'htlcMonitor') {
						const htlcWarnThreshold = this.getNodeParameter('htlcWarningThreshold', i) as number;
						const dustThreshold = this.getNodeParameter('dustThresholdSat', i) as number;

						const channelsResp = await lndRequest(this, 'GET', '/v1/channels', null, restHost, macaroon, tlsCert);
						const chans = channelsResp.channels || [];

						let totalPendingHtlcs = 0;
						let totalDustHtlcs = 0;
						const maxHtlcPerSpec = 483;

						const analyzed = chans.map((c: any) => {
							const pendingHtlcs = c.pending_htlcs || [];
							const numPending = pendingHtlcs.length;
							totalPendingHtlcs += numPending;

							const dustHtlcs = pendingHtlcs.filter((h: any) => {
								const amtSat = parseInt(h.amount || '0');
								return amtSat <= dustThreshold;
							});
							totalDustHtlcs += dustHtlcs.length;

							const htlcUtilizationPct = Math.round((numPending / maxHtlcPerSpec) * 100);

							let riskLevel = 'normal';
							if (numPending >= htlcWarnThreshold) riskLevel = 'warning';
							if (numPending >= maxHtlcPerSpec * 0.9) riskLevel = 'critical';
							if (dustHtlcs.length > numPending * 0.5 && numPending > 10) riskLevel = 'dust_attack_suspected';

							const dustAmountSat = dustHtlcs.reduce((s: number, h: any) => s + parseInt(h.amount || '0'), 0);

							return {
								chan_id: c.chan_id,
								channel_point: c.channel_point,
								remote_pubkey: c.remote_pubkey,
								capacity_sat: parseInt(c.capacity || '0'),
								pending_htlc_count: numPending,
								max_htlc_per_spec: maxHtlcPerSpec,
								htlc_utilization_pct: htlcUtilizationPct,
								dust_htlc_count: dustHtlcs.length,
								dust_htlc_pct: numPending > 0 ? Math.round((dustHtlcs.length / numPending) * 100) : 0,
								dust_amount_locked_sat: dustAmountSat,
								risk_level: riskLevel,
								active: c.active,
								pending_htlcs: pendingHtlcs.map((h: any) => ({
									incoming: h.incoming,
									amount_sat: parseInt(h.amount || '0'),
									hash_lock: h.hash_lock,
									expiration_height: h.expiration_height,
									is_dust: parseInt(h.amount || '0') <= dustThreshold,
								})),
							};
						});

						const atRisk = analyzed.filter((c: any) => c.risk_level !== 'normal');
						const dustAttackSuspected = analyzed.filter((c: any) => c.risk_level === 'dust_attack_suspected');

						responseData = {
							total_channels: analyzed.length,
							total_pending_htlcs: totalPendingHtlcs,
							total_dust_htlcs: totalDustHtlcs,
							dust_threshold_sat: dustThreshold,
							htlc_warning_threshold: htlcWarnThreshold,
							channels_at_risk: atRisk.length,
							dust_attack_suspected: dustAttackSuspected.length,
							alert: dustAttackSuspected.length > 0
								? `DUST ATTACK SUSPECTED on ${dustAttackSuspected.length} channel(s)!`
								: atRisk.length > 0
								? `${atRisk.length} channel(s) have high HTLC count`
								: 'All channels healthy',
							channels: analyzed,
							alerts: atRisk,
						};
					} else if (operation === 'updateMinHtlc') {
						const cp = this.getNodeParameter('minHtlcChannelPoint', i) as string;
						const minHtlcMsat = this.getNodeParameter('minHtlcMsat', i) as number;

						const feeReport = await lndRequest(this, 'GET', '/v1/fees', null, restHost, macaroon, tlsCert);
						const policies = feeReport.channel_fees || [];

						if (cp && cp.trim() !== '') {
							const [fundingTxid, outputIndex] = cp.split(':');
							const currentPolicy = policies.find((p: any) => p.channel_point === cp);
							const currentBase = currentPolicy ? parseInt(currentPolicy.base_fee_msat || '1000') : 1000;
							const currentRate = currentPolicy ? parseInt(currentPolicy.fee_per_mil || '100') : 100;

							const body = {
								chan_point: {
									funding_txid_str: fundingTxid,
									output_index: parseInt(outputIndex),
								},
								base_fee_msat: currentBase.toString(),
								fee_rate_ppm: currentRate.toString(),
								time_lock_delta: 40,
								min_htlc_msat: minHtlcMsat.toString(),
								min_htlc_msat_specified: true,
							};

							await lndRequest(this, 'POST', '/v1/chanpolicy', body, restHost, macaroon, tlsCert);

							responseData = {
								updated: 1,
								channel_point: cp,
								min_htlc_msat: minHtlcMsat,
								min_htlc_sat: Math.ceil(minHtlcMsat / 1000),
								preserved_base_fee_msat: currentBase,
								preserved_fee_rate_ppm: currentRate,
							};
						} else {
							// Global: preserve nothing, warn user
							const body = {
								global: true,
								base_fee_msat: '1000',
								fee_rate_ppm: '100',
								time_lock_delta: 40,
								min_htlc_msat: minHtlcMsat.toString(),
								min_htlc_msat_specified: true,
							};

							await lndRequest(this, 'POST', '/v1/chanpolicy', body, restHost, macaroon, tlsCert);

							responseData = {
								updated: 'all_channels',
								min_htlc_msat: minHtlcMsat,
								min_htlc_sat: Math.ceil(minHtlcMsat / 1000),
								warning: 'Global update applied. base_fee and fee_rate reset to defaults (1000 msat / 100 ppm). Use Update Channel Fee to adjust individually.',
							};
						}
					}
				}

				// ============ ROUTING ============
				else if (resource === 'routing') {
					if (operation === 'forwardingHistory') {
						const { startTime, endTime } = getTimeRange(this, i);
						const events = await getAllForwardingEvents(this, startTime, endTime, restHost, macaroon, tlsCert);

						const totalFeeMsat = events.reduce((s: number, e: any) => s + parseInt(e.fee_msat || '0'), 0);
						const totalAmtIn = events.reduce((s: number, e: any) => s + parseInt(e.amt_in_msat || '0'), 0);
						const totalAmtOut = events.reduce((s: number, e: any) => s + parseInt(e.amt_out_msat || '0'), 0);

						responseData = {
							count: events.length,
							total_fee_earned_msat: totalFeeMsat,
							total_fee_earned_sat: Math.floor(totalFeeMsat / 1000),
							total_amount_in_msat: totalAmtIn,
							total_amount_out_msat: totalAmtOut,
							start_time: startTime,
							end_time: endTime,
							forwarding_events: events,
						};
					} else if (operation === 'forwardingSummary') {
						const { startTime, endTime } = getTimeRange(this, i);
						const events = await getAllForwardingEvents(this, startTime, endTime, restHost, macaroon, tlsCert);

						// Aggregate by channel
						const chanStats: Record<string, any> = {};
						for (const e of events) {
							const inChan = e.chan_id_in || 'unknown';
							const outChan = e.chan_id_out || 'unknown';
							const feeMsat = parseInt(e.fee_msat || '0');
							const amtIn = parseInt(e.amt_in_msat || '0');
							const amtOut = parseInt(e.amt_out_msat || '0');

							if (!chanStats[inChan]) {
								chanStats[inChan] = {
									chan_id: inChan,
									forwards_in: 0,
									forwards_out: 0,
									fee_earned_msat: 0,
									amount_routed_in_msat: 0,
									amount_routed_out_msat: 0,
								};
							}
							if (!chanStats[outChan]) {
								chanStats[outChan] = {
									chan_id: outChan,
									forwards_in: 0,
									forwards_out: 0,
									fee_earned_msat: 0,
									amount_routed_in_msat: 0,
									amount_routed_out_msat: 0,
								};
							}

							chanStats[inChan].forwards_in++;
							chanStats[inChan].amount_routed_in_msat += amtIn;
							chanStats[outChan].forwards_out++;
							chanStats[outChan].fee_earned_msat += feeMsat;
							chanStats[outChan].amount_routed_out_msat += amtOut;
						}

						const summary = Object.values(chanStats)
							.map((s: any) => ({
								...s,
								fee_earned_sat: Math.floor(s.fee_earned_msat / 1000),
								total_forwards: s.forwards_in + s.forwards_out,
							}))
							.sort((a: any, b: any) => b.fee_earned_msat - a.fee_earned_msat);

						const totalFee = summary.reduce((s: number, c: any) => s + c.fee_earned_msat, 0);

						responseData = {
							total_events: events.length,
							total_fee_earned_msat: totalFee,
							total_fee_earned_sat: Math.floor(totalFee / 1000),
							unique_channels: summary.length,
							start_time: startTime,
							end_time: endTime,
							channel_summary: summary,
						};
					} else if (operation === 'queryRoutes') {
						const dest = this.getNodeParameter('destPubkey', i) as string;
						const amt = this.getNodeParameter('routeAmtSat', i) as number;
						responseData = await lndRequest(
							this, 'GET',
							`/v1/graph/routes/${dest}/${amt}`,
							null, restHost, macaroon, tlsCert,
						);
					} else if (operation === 'dustAnalysis') {
						const dustTimeRange = this.getNodeParameter('dustTimeRange', i) as string;
						const dustThresholdSat = this.getNodeParameter('dustAnalysisThresholdSat', i) as number;
						const suspiciousThreshold = this.getNodeParameter('suspiciousRateThreshold', i) as number;

						const now = Math.floor(Date.now() / 1000);
						let startTime: number;
						switch (dustTimeRange) {
							case '1h': startTime = now - 3600; break;
							case '24h': startTime = now - 86400; break;
							case '7d': startTime = now - 604800; break;
							case '30d': startTime = now - 2592000; break;
							default: startTime = now - 86400;
						}

						const events = await getAllForwardingEvents(this, startTime, now, restHost, macaroon, tlsCert);

						// Separate dust vs normal forwards
						const dustEvents = events.filter((e: any) => {
							const amtOutSat = Math.floor(parseInt(e.amt_out_msat || '0') / 1000);
							return amtOutSat <= dustThresholdSat;
						});

						// Aggregate dust per channel pair (in -> out)
						const chanDustStats: Record<string, any> = {};
						for (const e of dustEvents) {
							const inChan = e.chan_id_in || 'unknown';
							const outChan = e.chan_id_out || 'unknown';
							const key = `${inChan}->${outChan}`;
							const amtOutMsat = parseInt(e.amt_out_msat || '0');
							const feeMsat = parseInt(e.fee_msat || '0');

							if (!chanDustStats[key]) {
								chanDustStats[key] = {
									route: key,
									chan_id_in: inChan,
									chan_id_out: outChan,
									dust_forward_count: 0,
									total_dust_amount_msat: 0,
									total_dust_fee_msat: 0,
									min_amount_sat: Infinity,
									max_amount_sat: 0,
								};
							}
							chanDustStats[key].dust_forward_count++;
							chanDustStats[key].total_dust_amount_msat += amtOutMsat;
							chanDustStats[key].total_dust_fee_msat += feeMsat;
							const amtSat = Math.floor(amtOutMsat / 1000);
							if (amtSat < chanDustStats[key].min_amount_sat) chanDustStats[key].min_amount_sat = amtSat;
							if (amtSat > chanDustStats[key].max_amount_sat) chanDustStats[key].max_amount_sat = amtSat;
						}

						// Also aggregate per individual channel (inbound)
						const inboundDust: Record<string, number> = {};
						for (const e of dustEvents) {
							const inChan = e.chan_id_in || 'unknown';
							inboundDust[inChan] = (inboundDust[inChan] || 0) + 1;
						}

						const suspiciousRoutes = Object.values(chanDustStats)
							.filter((r: any) => r.dust_forward_count >= suspiciousThreshold)
							.map((r: any) => ({
								...r,
								total_dust_amount_sat: Math.floor(r.total_dust_amount_msat / 1000),
								total_dust_fee_sat: Math.floor(r.total_dust_fee_msat / 1000),
								min_amount_sat: r.min_amount_sat === Infinity ? 0 : r.min_amount_sat,
								avg_amount_sat: r.dust_forward_count > 0
									? Math.floor(r.total_dust_amount_msat / r.dust_forward_count / 1000)
									: 0,
								suspicious: true,
							}))
							.sort((a: any, b: any) => b.dust_forward_count - a.dust_forward_count);

						const suspiciousInbound = Object.entries(inboundDust)
							.filter(([_, count]) => count >= suspiciousThreshold)
							.map(([chanId, count]) => ({ chan_id_in: chanId, dust_count: count }))
							.sort((a, b) => b.dust_count - a.dust_count);

						const totalDustFee = dustEvents.reduce((s: number, e: any) => s + parseInt(e.fee_msat || '0'), 0);
						const totalNormalFee = events
							.filter((e: any) => Math.floor(parseInt(e.amt_out_msat || '0') / 1000) > dustThresholdSat)
							.reduce((s: number, e: any) => s + parseInt(e.fee_msat || '0'), 0);

						responseData = {
							period: dustTimeRange,
							dust_threshold_sat: dustThresholdSat,
							suspicious_rate_threshold: suspiciousThreshold,
							total_forwards: events.length,
							total_dust_forwards: dustEvents.length,
							dust_percentage: events.length > 0
								? Math.round((dustEvents.length / events.length) * 100)
								: 0,
							total_dust_fee_earned_msat: totalDustFee,
							total_dust_fee_earned_sat: Math.floor(totalDustFee / 1000),
							total_normal_fee_earned_msat: totalNormalFee,
							total_normal_fee_earned_sat: Math.floor(totalNormalFee / 1000),
							fee_efficiency_warning: totalDustFee > 0 && totalNormalFee > 0
								? `Dust forwards represent ${Math.round((totalDustFee / (totalDustFee + totalNormalFee)) * 100)}% of fee revenue but may cost more in force-close risk`
								: null,
							suspicious_routes_count: suspiciousRoutes.length,
							suspicious_inbound_channels: suspiciousInbound.length,
							alert: suspiciousRoutes.length > 0
								? `DUST ATTACK WARNING: ${suspiciousRoutes.length} route(s) with ${suspiciousThreshold}+ dust forwards detected`
								: 'No suspicious dust patterns detected',
							recommendations: suspiciousInbound.length > 0
								? suspiciousInbound.map((c: any) => ({
									action: 'increase_min_htlc',
									chan_id: c.chan_id_in,
									reason: `${c.dust_count} dust forwards received via this channel`,
									suggested_min_htlc_msat: dustThresholdSat * 1000,
								}))
								: [],
							suspicious_routes: suspiciousRoutes,
							suspicious_inbound_details: suspiciousInbound,
						};
					}
				}

				// ============ FEES ============
				else if (resource === 'fees') {
					if (operation === 'feeReport') {
						responseData = await lndRequest(this, 'GET', '/v1/fees', null, restHost, macaroon, tlsCert);
					} else if (operation === 'updateFee') {
						const cp = this.getNodeParameter('feeChannelPoint', i) as string;
						const baseFee = this.getNodeParameter('baseFee', i) as number;
						const feeRatePpm = this.getNodeParameter('feeRate', i) as number;
						const timeLockDelta = this.getNodeParameter('timeLockDelta', i) as number;

						const body: any = {
							base_fee_msat: baseFee.toString(),
							fee_rate_ppm: feeRatePpm.toString(),
							time_lock_delta: timeLockDelta,
						};

						if (cp && cp.trim() !== '') {
							const [fundingTxid, outputIndex] = cp.split(':');
							body.chan_point = {
								funding_txid_str: fundingTxid,
								output_index: parseInt(outputIndex),
							};
						} else {
							body.global = true;
						}

						responseData = await lndRequest(this, 'POST', '/v1/chanpolicy', body, restHost, macaroon, tlsCert);
					} else if (operation === 'autoFeeSuggestion') {
						const lowFee = this.getNodeParameter('lowBalanceFee', i) as number;
						const balFee = this.getNodeParameter('balancedFee', i) as number;
						const highFee = this.getNodeParameter('highBalanceFee', i) as number;
						const autoBase = this.getNodeParameter('autoBaseFee', i) as number;
						const apply = this.getNodeParameter('applySuggestions', i) as boolean;

						const channelsResp = await lndRequest(this, 'GET', '/v1/channels', null, restHost, macaroon, tlsCert);
						const chans = channelsResp.channels || [];

						const suggestions = chans.map((c: any) => {
							const cap = parseInt(c.capacity || '0');
							const local = parseInt(c.local_balance || '0');
							const ratio = cap > 0 ? Math.round((local / cap) * 100) : 0;

							let suggestedPpm: number;
							let reason: string;
							if (ratio < 30) {
								suggestedPpm = lowFee;
								reason = 'Low local balance - high fee to discourage drain';
							} else if (ratio > 70) {
								suggestedPpm = highFee;
								reason = 'High local balance - low fee to encourage outflow';
							} else {
								suggestedPpm = balFee;
								reason = 'Well balanced - standard fee';
							}

							return {
								chan_id: c.chan_id,
								channel_point: c.channel_point,
								remote_pubkey: c.remote_pubkey,
								capacity_sat: cap,
								local_balance_sat: local,
								local_ratio_pct: ratio,
								suggested_fee_rate_ppm: suggestedPpm,
								suggested_base_fee_msat: autoBase,
								reason,
							};
						});

						// Apply if requested
						if (apply) {
							for (const s of suggestions) {
								const [fundingTxid, outputIndex] = s.channel_point.split(':');
								await lndRequest(
									this, 'POST', '/v1/chanpolicy',
									{
										chan_point: {
											funding_txid_str: fundingTxid,
											output_index: parseInt(outputIndex),
										},
										base_fee_msat: s.suggested_base_fee_msat.toString(),
										fee_rate_ppm: s.suggested_fee_rate_ppm.toString(),
										time_lock_delta: 40,
									},
									restHost, macaroon, tlsCert,
								);
							}
						}

						responseData = {
							total_channels: suggestions.length,
							applied: apply,
							suggestions,
						};
					}
				}

				// ============ PAYMENT ============
				else if (resource === 'payment') {
					if (operation === 'createInvoice') {
						const amt = this.getNodeParameter('amount', i) as number;
						const memo = this.getNodeParameter('memo', i) as string;
						const exp = this.getNodeParameter('expiry', i) as number;
						responseData = await lndRequest(
							this, 'POST', '/v1/invoices',
							{
								value: amt.toString(),
								memo,
								expiry: exp.toString(),
							},
							restHost, macaroon, tlsCert,
						);
					} else if (operation === 'sendPayment') {
						const payReq = this.getNodeParameter('paymentRequest', i) as string;
						const timeoutSec = this.getNodeParameter('timeout', i) as number;
						const feeLimit = this.getNodeParameter('feeLimitSat', i) as number;
						responseData = await lndRequest(
							this, 'POST', '/v2/router/send',
							{
								payment_request: payReq,
								timeout_seconds: timeoutSec,
								fee_limit_sat: feeLimit.toString(),
							},
							restHost, macaroon, tlsCert,
						);
					} else if (operation === 'listInvoices') {
						responseData = await lndRequest(this, 'GET', '/v1/invoices?reversed=true&num_max_invoices=100', null, restHost, macaroon, tlsCert);
					} else if (operation === 'listPayments') {
						responseData = await lndRequest(this, 'GET', '/v1/payments?reversed=true&max_payments=100', null, restHost, macaroon, tlsCert);
					} else if (operation === 'decodeInvoice') {
						const payReq = this.getNodeParameter('decodePayReq', i) as string;
						responseData = await lndRequest(this, 'GET', `/v1/payreq/${payReq}`, null, restHost, macaroon, tlsCert);
					}
				}

				// ============ PEER ============
				else if (resource === 'peer') {
					if (operation === 'listPeers') {
						responseData = await lndRequest(this, 'GET', '/v1/peers', null, restHost, macaroon, tlsCert);
					} else if (operation === 'connectPeer') {
						const addr = this.getNodeParameter('peerAddress', i) as string;
						const [pubkey, host] = addr.split('@');
						responseData = await lndRequest(
							this, 'POST', '/v1/peers',
							{
								addr: { pubkey, host },
								perm: true,
							},
							restHost, macaroon, tlsCert,
						);
					} else if (operation === 'disconnectPeer') {
						const pubkey = this.getNodeParameter('disconnectPubkey', i) as string;
						responseData = await lndRequest(this, 'DELETE', `/v1/peers/${pubkey}`, null, restHost, macaroon, tlsCert);
					} else if (operation === 'getNodeInfo') {
						const pubkey = this.getNodeParameter('nodeInfoPubkey', i) as string;
						responseData = await lndRequest(this, 'GET', `/v1/graph/node/${pubkey}`, null, restHost, macaroon, tlsCert);
					} else if (operation === 'peerScoring') {
						// Gather channels + forwarding data (last 30 days)
						const channelsResp = await lndRequest(this, 'GET', '/v1/channels', null, restHost, macaroon, tlsCert);
						const chans = channelsResp.channels || [];

						const now = Math.floor(Date.now() / 1000);
						const thirtyDaysAgo = now - 2592000;
						const events = await getAllForwardingEvents(this, thirtyDaysAgo, now, restHost, macaroon, tlsCert);

						// Aggregate forwarding revenue per channel
						const chanRevenue: Record<string, number> = {};
						const chanForwards: Record<string, number> = {};
						for (const e of events) {
							const outChan = e.chan_id_out || '';
							const inChan = e.chan_id_in || '';
							const fee = parseInt(e.fee_msat || '0');

							chanRevenue[outChan] = (chanRevenue[outChan] || 0) + fee;
							chanForwards[outChan] = (chanForwards[outChan] || 0) + 1;
							chanForwards[inChan] = (chanForwards[inChan] || 0) + 1;
						}

						// Build peer scores
						const peerMap: Record<string, any> = {};
						for (const c of chans) {
							const pk = c.remote_pubkey;
							if (!peerMap[pk]) {
								peerMap[pk] = {
									pubkey: pk,
									channels: [],
									total_capacity_sat: 0,
									total_local_balance_sat: 0,
									total_revenue_msat: 0,
									total_forwards: 0,
									avg_uptime_pct: 0,
								};
							}
							const cap = parseInt(c.capacity || '0');
							const local = parseInt(c.local_balance || '0');
							const chanId = c.chan_id;
							const rev = chanRevenue[chanId] || 0;
							const fwd = chanForwards[chanId] || 0;
							const uptimePct = c.lifetime && parseInt(c.lifetime) > 0
								? Math.round((parseInt(c.uptime || '0') / parseInt(c.lifetime)) * 100)
								: 0;

							peerMap[pk].channels.push(chanId);
							peerMap[pk].total_capacity_sat += cap;
							peerMap[pk].total_local_balance_sat += local;
							peerMap[pk].total_revenue_msat += rev;
							peerMap[pk].total_forwards += fwd;
							peerMap[pk].avg_uptime_pct += uptimePct;
						}

						const peerScores = Object.values(peerMap).map((p: any) => {
							const numChans = p.channels.length;
							p.avg_uptime_pct = numChans > 0 ? Math.round(p.avg_uptime_pct / numChans) : 0;
							p.total_revenue_sat = Math.floor(p.total_revenue_msat / 1000);
							p.revenue_per_million_capacity =
								p.total_capacity_sat > 0
									? Math.round((p.total_revenue_msat / p.total_capacity_sat) * 1000)
									: 0;
							// Score: weighted combo of revenue, uptime, and forward count
							p.score = Math.round(
								(p.revenue_per_million_capacity * 0.5) +
								(p.avg_uptime_pct * 0.3) +
								(Math.min(p.total_forwards, 1000) * 0.2),
							);
							p.num_channels = numChans;
							return p;
						}).sort((a: any, b: any) => b.score - a.score);

						responseData = {
							total_peers_scored: peerScores.length,
							period_days: 30,
							peer_scores: peerScores,
						};
					}
				}

				// ============ REBALANCE ============
				else if (resource === 'rebalance') {
					if (operation === 'suggestRebalances') {
						const targetRatio = this.getNodeParameter('targetRatio', i) as number;
						const minDeviation = this.getNodeParameter('minDeviation', i) as number;

						const channelsResp = await lndRequest(this, 'GET', '/v1/channels', null, restHost, macaroon, tlsCert);
						const chans = channelsResp.channels || [];

						const suggestions: any[] = [];
						const sources: any[] = []; // High local balance
						const sinks: any[] = []; // Low local balance

						for (const c of chans) {
							if (!c.active) continue;
							const cap = parseInt(c.capacity || '0');
							const local = parseInt(c.local_balance || '0');
							const ratio = cap > 0 ? Math.round((local / cap) * 100) : 0;
							const deviation = Math.abs(ratio - targetRatio);

							if (deviation >= minDeviation) {
								const idealLocal = Math.round(cap * (targetRatio / 100));
								const diff = local - idealLocal;
								const entry = {
									chan_id: c.chan_id,
									channel_point: c.channel_point,
									remote_pubkey: c.remote_pubkey,
									capacity_sat: cap,
									local_balance_sat: local,
									local_ratio_pct: ratio,
									target_ratio_pct: targetRatio,
									deviation_pct: deviation,
									amount_to_move_sat: Math.abs(diff),
								};
								if (diff > 0) {
									sources.push(entry);
								} else {
									sinks.push(entry);
								}
							}
						}

						// Pair sources with sinks
						for (let si = 0; si < Math.min(sources.length, sinks.length); si++) {
							const amount = Math.min(sources[si].amount_to_move_sat, sinks[si].amount_to_move_sat);
							suggestions.push({
								from_channel: sources[si].chan_id,
								from_pubkey: sources[si].remote_pubkey,
								from_local_ratio_pct: sources[si].local_ratio_pct,
								to_channel: sinks[si].chan_id,
								to_pubkey: sinks[si].remote_pubkey,
								to_local_ratio_pct: sinks[si].local_ratio_pct,
								suggested_amount_sat: amount,
							});
						}

						responseData = {
							target_ratio_pct: targetRatio,
							min_deviation_pct: minDeviation,
							channels_need_outflow: sources.length,
							channels_need_inflow: sinks.length,
							suggested_rebalances: suggestions,
							sources,
							sinks,
						};
					} else if (operation === 'circularRebalance') {
						const outChanId = this.getNodeParameter('outgoingChanId', i) as string;
						const lastHopPubkey = this.getNodeParameter('lastHopPubkey', i) as string;
						const amount = this.getNodeParameter('rebalanceAmount', i) as number;
						const maxFee = this.getNodeParameter('rebalanceMaxFee', i) as number;

						// Create invoice to self
						const invoice = await lndRequest(
							this, 'POST', '/v1/invoices',
							{
								value: amount.toString(),
								memo: `Rebalance ${outChanId}`,
								expiry: '600',
							},
							restHost, macaroon, tlsCert,
						);

						const payReq = invoice.payment_request;

						// Pay the self-invoice with route hints
						const lastHopBytes = Buffer.from(lastHopPubkey, 'hex').toString('base64');
						const payResult = await lndRequest(
							this, 'POST', '/v2/router/send',
							{
								payment_request: payReq,
								timeout_seconds: 120,
								fee_limit_sat: maxFee.toString(),
								outgoing_chan_id: outChanId,
								last_hop_pubkey: lastHopBytes,
							},
							restHost, macaroon, tlsCert,
						);

						responseData = {
							invoice_payment_request: payReq,
							outgoing_channel: outChanId,
							last_hop_pubkey: lastHopPubkey,
							amount_sat: amount,
							max_fee_sat: maxFee,
							payment_result: payResult,
						};
					}
				}

				// ============ WALLET ============
				else if (resource === 'wallet') {
					if (operation === 'listUtxos') {
						// Use walletkit ListUnspent - min_confs=0 to get all
						const utxosResp = await lndRequest(
							this, 'POST', '/v2/wallet/utxos',
							{ min_confs: 0, max_confs: 2147483647 },
							restHost, macaroon, tlsCert,
						);

						const utxos = (utxosResp.utxos || []).map((u: any) => ({
							txid: u.outpoint?.txid_str || '',
							output_index: u.outpoint?.output_index || 0,
							outpoint: `${u.outpoint?.txid_str || ''}:${u.outpoint?.output_index || 0}`,
							amount_sat: parseInt(u.amount_sat || '0'),
							address: u.address || '',
							address_type: u.address_type || '',
							confirmations: parseInt(u.confirmations || '0'),
							pk_script: u.pk_script || '',
						}));

						const totalBalance = utxos.reduce((s: number, u: any) => s + u.amount_sat, 0);

						responseData = {
							total_utxos: utxos.length,
							total_balance_sat: totalBalance,
							total_balance_btc: (totalBalance / 100000000).toFixed(8),
							utxos,
						};
					} else if (operation === 'detectDustUtxos') {
						const dustThreshold = this.getNodeParameter('utxoDustThreshold', i) as number;
						const autoFreeze = this.getNodeParameter('autoFreezeDust', i) as boolean;

						const utxosResp = await lndRequest(
							this, 'POST', '/v2/wallet/utxos',
							{ min_confs: 0, max_confs: 2147483647 },
							restHost, macaroon, tlsCert,
						);

						const allUtxos = utxosResp.utxos || [];
						const dustUtxos = allUtxos.filter((u: any) => parseInt(u.amount_sat || '0') <= dustThreshold);
						const normalUtxos = allUtxos.filter((u: any) => parseInt(u.amount_sat || '0') > dustThreshold);

						const dustDetails = dustUtxos.map((u: any) => ({
							txid: u.outpoint?.txid_str || '',
							output_index: u.outpoint?.output_index || 0,
							outpoint: `${u.outpoint?.txid_str || ''}:${u.outpoint?.output_index || 0}`,
							amount_sat: parseInt(u.amount_sat || '0'),
							address: u.address || '',
							confirmations: parseInt(u.confirmations || '0'),
							risk: 'dust_attack_likely',
							recommendation: 'Do NOT spend - freeze this UTXO',
						}));

						const frozenResults: any[] = [];
						if (autoFreeze && dustDetails.length > 0) {
							const freezeDuration = this.getNodeParameter('freezeDuration', i) as number;
							// Generate a unique lease ID
							const leaseId = Buffer.from('n8n-dust-protect').toString('base64');

							for (const dust of dustUtxos) {
								try {
									const txidBytes = Buffer.from(dust.outpoint?.txid_str || '', 'hex').toString('base64');
									await lndRequest(
										this, 'POST', '/v2/wallet/utxos/lease',
										{
											id: leaseId,
											outpoint: {
												txid_bytes: txidBytes,
												output_index: dust.outpoint?.output_index || 0,
											},
											expiration_seconds: freezeDuration.toString(),
										},
										restHost, macaroon, tlsCert,
									);
									frozenResults.push({
										outpoint: `${dust.outpoint?.txid_str}:${dust.outpoint?.output_index}`,
										status: 'frozen',
										lease_duration_seconds: freezeDuration,
										lease_duration_days: Math.round(freezeDuration / 86400),
									});
								} catch (leaseErr: any) {
									frozenResults.push({
										outpoint: `${dust.outpoint?.txid_str}:${dust.outpoint?.output_index}`,
										status: 'freeze_failed',
										error: leaseErr.message || 'Unknown error',
									});
								}
							}
						}

						const totalDustAmount = dustDetails.reduce((s: number, d: any) => s + d.amount_sat, 0);
						const totalNormalAmount = normalUtxos.reduce((s: number, u: any) => s + parseInt(u.amount_sat || '0'), 0);

						responseData = {
							dust_threshold_sat: dustThreshold,
							total_utxos: allUtxos.length,
							dust_utxos_count: dustDetails.length,
							normal_utxos_count: normalUtxos.length,
							dust_total_sat: totalDustAmount,
							normal_total_sat: totalNormalAmount,
							alert: dustDetails.length > 0
								? `${dustDetails.length} dust UTXO(s) detected (${totalDustAmount} sats) - potential dust attack`
								: 'No dust UTXOs detected',
							auto_freeze_enabled: autoFreeze,
							frozen_results: frozenResults.length > 0 ? frozenResults : undefined,
							dust_utxos: dustDetails,
						};
					} else if (operation === 'leaseOutput') {
						const outpoint = this.getNodeParameter('leaseOutpoint', i) as string;
						const duration = this.getNodeParameter('leaseDuration', i) as number;
						const [txid, outputIndexStr] = outpoint.split(':');
						const outputIndex = parseInt(outputIndexStr);

						const leaseId = Buffer.from('n8n-freeze').toString('base64');
						const txidBytes = Buffer.from(txid, 'hex').toString('base64');

						const result = await lndRequest(
							this, 'POST', '/v2/wallet/utxos/lease',
							{
								id: leaseId,
								outpoint: {
									txid_bytes: txidBytes,
									output_index: outputIndex,
								},
								expiration_seconds: duration.toString(),
							},
							restHost, macaroon, tlsCert,
						);

						responseData = {
							outpoint,
							status: 'frozen',
							lease_duration_seconds: duration,
							lease_duration_days: Math.round(duration / 86400),
							expiration: result.expiration || 'unknown',
						};
					} else if (operation === 'releaseOutput') {
						const outpoint = this.getNodeParameter('releaseOutpoint', i) as string;
						const [txid, outputIndexStr] = outpoint.split(':');
						const outputIndex = parseInt(outputIndexStr);

						const leaseId = Buffer.from('n8n-freeze').toString('base64');
						const txidBytes = Buffer.from(txid, 'hex').toString('base64');

						await lndRequest(
							this, 'POST', '/v2/wallet/utxos/release',
							{
								id: leaseId,
								outpoint: {
									txid_bytes: txidBytes,
									output_index: outputIndex,
								},
							},
							restHost, macaroon, tlsCert,
						);

						responseData = {
							outpoint,
							status: 'unfrozen',
						};
					}
				}

				const executionData = this.helpers.constructExecutionMetaData(
					this.helpers.returnJsonArray(responseData as any),
					{ itemData: { item: i } },
				);

				returnData.push(...executionData);
			} catch (error) {
				if (this.continueOnFail()) {
					const errorMessage = error instanceof Error ? error.message : 'Unknown error';
					const executionErrorData = this.helpers.constructExecutionMetaData(
						this.helpers.returnJsonArray({ error: errorMessage }),
						{ itemData: { item: i } },
					);
					returnData.push(...executionErrorData);
					continue;
				}
				throw error;
			}
		}

		return [returnData];
	}
}
