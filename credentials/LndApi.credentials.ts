import {
	ICredentialType,
	INodeProperties,
} from 'n8n-workflow';

export class LndApi implements ICredentialType {
	name = 'lndApi';
	displayName = 'LND API';
	documentationUrl = 'https://docs.lightning.engineering/lightning-network-tools/lnd/';
	properties: INodeProperties[] = [
		{
			displayName: 'REST Host',
			name: 'restHost',
			type: 'string',
			default: 'https://localhost:8080',
			description: 'The LND REST server URL (e.g. https://localhost:8080)',
		},
		{
			displayName: 'Macaroon',
			name: 'macaroon',
			type: 'string',
			typeOptions: {
				password: true,
			},
			default: '',
			description: 'The macaroon in hexadecimal format for authentication (admin.macaroon)',
		},
		{
			displayName: 'TLS Certificate',
			name: 'tlsCert',
			type: 'string',
			typeOptions: {
				password: true,
			},
			default: '',
			description: 'The TLS certificate (content of tls.cert file). Leave empty to accept self-signed certificates.',
		},
	];
}
