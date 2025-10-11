import {
	IExecuteFunctions,
	INodeExecutionData,
	INodeType,
	INodeTypeDescription,
	NodeOperationError,
} from 'n8n-workflow';

import * as https from 'https';

export class LightningNetwork implements INodeType {
	description: INodeTypeDescription = {
		displayName: 'Lightning Network (LND)',
		name: 'lightningNetwork',
		icon: 'file:lightning.svg',
		group: ['transform'],
		version: 1,
		subtitle: '={{$parameter["operation"]}}',
		description: 'Interact with Lightning Network Daemon (LND)',
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
			{
				displayName: 'Operation',
				name: 'operation',
				type: 'options',
				noDataExpression: true,
				options: [
					{
						name: 'Get Info',
						value: 'getInfo',
						description: 'Get node information (channels, version, etc.)',
						action: 'Get node information',
					},
					{
						name: 'Count Forwarding',
						value: 'countForwarding',
						description: 'Count forwarding events from the last 24 hours',
						action: 'Count forwarding from yesterday',
					},
					{
						name: 'Create Invoice',
						value: 'createInvoice',
						description: 'Create a Lightning invoice',
						action: 'Create an invoice',
					},
					{
						name: 'Send Payment',
						value: 'sendPayment',
						description: 'Send a Lightning payment',
						action: 'Send a payment',
					},
				],
				default: 'getInfo',
			},
			// Parameters for creating an invoice
			{
				displayName: 'Amount (satoshis)',
				name: 'amount',
				type: 'number',
				displayOptions: {
					show: {
						operation: ['createInvoice'],
					},
				},
				default: 1000,
				description: 'Invoice amount in satoshis',
			},
			{
				displayName: 'Memo',
				name: 'memo',
				type: 'string',
				displayOptions: {
					show: {
						operation: ['createInvoice'],
					},
				},
				default: '',
				description: 'Invoice description',
			},
			{
				displayName: 'Expiry (seconds)',
				name: 'expiry',
				type: 'number',
				displayOptions: {
					show: {
						operation: ['createInvoice'],
					},
				},
				default: 3600,
				description: 'Invoice validity duration in seconds',
			},
			// Parameters for sending a payment
			{
				displayName: 'Payment Request',
				name: 'paymentRequest',
				type: 'string',
				displayOptions: {
					show: {
						operation: ['sendPayment'],
					},
				},
				default: '',
				required: true,
				description: 'The Lightning invoice to pay (BOLT11)',
			},
			{
				displayName: 'Timeout (seconds)',
				name: 'timeout',
				type: 'number',
				displayOptions: {
					show: {
						operation: ['sendPayment'],
					},
				},
				default: 60,
				description: 'Maximum time for payment completion',
			},
		],
	};

	async execute(this: IExecuteFunctions): Promise<INodeExecutionData[][]> {
		const items = this.getInputData();
		const returnData: INodeExecutionData[] = [];
		const operation = this.getNodeParameter('operation', 0);

		// Get credentials
		const credentials = await this.getCredentials('lndApi');
		const restHost = credentials.restHost as string;
		const macaroon = credentials.macaroon as string;
		const tlsCert = credentials.tlsCert as string | undefined;

		for (let i = 0; i < items.length; i++) {
			try {
				let responseData;

				if (operation === 'getInfo') {
					responseData = await lndRequest.call(this, 'GET', '/v1/getinfo', {}, restHost, macaroon, tlsCert);
				}

				if (operation === 'countForwarding') {
					// Calculate timestamp for 24 hours ago
					const nowTimestamp = Math.floor(Date.now() / 1000);
					const yesterdayTimestamp = nowTimestamp - 86400; // 86400 = 24h in seconds

					const forwardingHistory = await lndRequest.call(
						this,
						'POST',
						'/v1/switch',
						{
							start_time: yesterdayTimestamp.toString(),
							end_time: nowTimestamp.toString(),
							num_max_events: 100000,
						},
						restHost,
						macaroon,
						tlsCert,
					);

					responseData = {
						count: forwardingHistory.forwarding_events?.length || 0,
						start_time: yesterdayTimestamp,
						end_time: nowTimestamp,
						forwarding_events: forwardingHistory.forwarding_events || [],
					};
				}

				if (operation === 'createInvoice') {
					const amount = this.getNodeParameter('amount', i) as number;
					const memo = this.getNodeParameter('memo', i) as string;
					const expiry = this.getNodeParameter('expiry', i) as number;

					responseData = await lndRequest.call(
						this,
						'POST',
						'/v1/invoices',
						{
							value: amount.toString(),
							memo,
							expiry: expiry.toString(),
						},
						restHost,
						macaroon,
						tlsCert,
					);
				}

				if (operation === 'sendPayment') {
					const paymentRequest = this.getNodeParameter('paymentRequest', i) as string;
					const timeout = this.getNodeParameter('timeout', i) as number;

					responseData = await lndRequest.call(
						this,
						'POST',
						'/v2/router/send',
						{
							payment_request: paymentRequest,
							timeout_seconds: timeout,
						},
						restHost,
						macaroon,
						tlsCert,
					);
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

async function lndRequest(
	this: IExecuteFunctions,
	method: string,
	endpoint: string,
	body: any,
	restHost: string,
	macaroon: string,
	tlsCert: string | undefined,
): Promise<any> {
	return new Promise((resolve, reject) => {
		const options: https.RequestOptions = {
			hostname: restHost.replace(/^https?:\/\//, '').split(':')[0],
			port: parseInt(restHost.split(':')[2] || '8080'),
			path: endpoint,
			method,
			headers: {
				'Grpc-Metadata-macaroon': macaroon,
				'Content-Type': 'application/json',
			},
		};

		// Add TLS certificate only if provided
		if (tlsCert && tlsCert.trim() !== '') {
			options.ca = Buffer.from(tlsCert, 'utf-8');
		} else {
			// If no certificate provided, allow self-signed certificates
			options.rejectUnauthorized = false;
		}

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
						reject(new NodeOperationError(
							this.getNode(),
							`LND API error: ${parsedData.error || data}`,
						));
					}
				} catch (error) {
					const errorMessage = error instanceof Error ? error.message : 'Unknown parsing error';
					reject(new NodeOperationError(
						this.getNode(),
						`Error parsing response: ${errorMessage}`,
					));
				}
			});
		});

		req.on('error', (error) => {
			const errorMessage = error instanceof Error ? error.message : 'Unknown connection error';
			reject(new NodeOperationError(
				this.getNode(),
				`Connection error to LND: ${errorMessage}`,
			));
		});

		if (method !== 'GET' && body) {
			req.write(JSON.stringify(body));
		}

		req.end();
	});
}
